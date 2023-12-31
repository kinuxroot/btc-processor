// 生成每年的新地址数量、新实体数量、活跃用户（实体）数量

#include "btc-config.h"
#include "final_export_address_statistics/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/btc_utils.h"
#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "fmt/format.h"
#include <nlohmann/json.hpp>
#include <argparse/argparse.hpp>

#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <future>
#include <set>
#include <filesystem>
#include <thread>
#include <iostream>
#include <map>
#include <limits>
#include <iomanip>
#include <algorithm>
#include <execution>

using json = nlohmann::json;
using CountList = std::vector<uint8_t>;
using EntityYearList = std::vector<int16_t>;

const auto INVALID_ENTITY_YEAR = std::numeric_limits<int16_t>::min();

namespace fs = std::filesystem;

inline BtcId parseMaxId(const char* maxIdArg);

std::vector<
    std::pair<std::string, std::vector<std::string>>
> groupDaysListByYears(const std::vector<std::string>& daysList, uint32_t startYear, uint32_t endYear);

static argparse::ArgumentParser createArgumentParser();

void generateAddressStatisticsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    const utils::btc::WeightedQuickUnion* quickUnion,
    CountList* addressCountList,
    CountList* entityCountList,
    CountList* activateEntityCountList
);

void calculateAddressStatisticsOfDays(
    const std::string& dayDir,
    const utils::btc::WeightedQuickUnion& quickUnion,
    CountList& addressCountList,
    CountList& entityCountList,
    CountList& activateEntityCountList
);

void calculateAddressStatisticsOfBlock(
    const json& block,
    const utils::btc::WeightedQuickUnion& quickUnion,
    CountList& addressCountList,
    CountList& entityCountList,
    CountList& activateEntityCountList
);

void calculateAddressStatisticsOfTx(
    const json& tx,
    const utils::btc::WeightedQuickUnion& quickUnion,
    CountList& addressCountList,
    CountList& entityCountList,
    CountList& activateEntityCountList
);

void processAddress(
    BtcId addressId,
    const utils::btc::WeightedQuickUnion& quickUnion,
    CountList& addressCountList,
    CountList& entityCountList,
    CountList& activateEntityCountList
);

std::size_t loadCountList(
    const std::string& inputFilePath,
    CountList& countList
);

void dumpCountList(
    const std::string& outputFilePath,
    CountList& countList
);

void dumpYearList(
    const std::string& outputFilePath,
    EntityYearList& yearList
);

void dumpSummary(
    const std::string& outputFilePath,
    size_t allAddressCount,
    size_t allEntityCount,
    size_t newAddressCount,
    size_t newEntityCount,
    size_t activateEntityCount
);

void dumpNewAddressFile(
    const std::string& outputFilePath,
    const std::vector<std::string>& id2address,
    const CountList& prevCountList,
    const CountList& currCountList
);

void dumpNewEntityFile(
    const std::string& outputFilePath,
    utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const CountList& prevCountList,
    const CountList& currCountList
);

void dumpActivateEntityFile(
    const std::string& outputFilePath,
    utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const CountList& activateEntityCountList
);

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    auto argumentParser = createArgumentParser();
    try {
        argumentParser.parse_args(argc, argv);
    }
    catch (const std::runtime_error& err) {
        logger.error(err.what());
        std::cerr << argumentParser;
        std::exit(1);
    }

    std::string daysListFilePath = argumentParser.get("days_dir_list");
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    std::vector<std::string> daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    std::string outputBaseDirPath = argumentParser.get("output_base_dir");
    fs::create_directories(outputBaseDirPath);

    fs::path addressOutputBaseDirPath = fs::path(outputBaseDirPath) / "address";
    fs::create_directories(addressOutputBaseDirPath);

    fs::path entityOutputBaseDirPath = fs::path(outputBaseDirPath) / "entity";
    fs::create_directories(entityOutputBaseDirPath);

    fs::path summaryOutputBaseDirPath = fs::path(outputBaseDirPath) / "summary";
    fs::create_directories(summaryOutputBaseDirPath);

    uint32_t workerCount = std::min(
        argumentParser.get<uint32_t>("--worker_count"),
        std::thread::hardware_concurrency()
    );
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    auto startYear = argumentParser.get<uint32_t>("--start_year");
    logger.info(fmt::format("Using start year: {}", startYear));

    auto endYear = argumentParser.get<uint32_t>("--end_year");
    logger.info(fmt::format("Using end year: {}", endYear));

    const std::string ufFilePath = argumentParser.get("--union_file");
    utils::btc::WeightedQuickUnion quickUnion(1);
    logger.info(fmt::format("Load quickUnion from {}", ufFilePath));
    quickUnion.load(ufFilePath);
    logger.info(fmt::format("Loaded quickUnion {} items from {}", quickUnion.getSize(), ufFilePath));

    logUsedMemory();

    std::string id2AddressFilePath = argumentParser.get("id2addr");
    logger.info("Load address2Id...");
    const auto& id2Address = utils::btc::loadId2Address(id2AddressFilePath.c_str());
    logger.info(fmt::format("Loaded id2Address: {} items", id2Address.size()));

    logUsedMemory();

    const std::string entityLabelFilePath = argumentParser.get("--entity_label_file");
    std::ifstream entityLabelFile(entityLabelFilePath, std::ios::binary);
    std::vector<utils::btc::ClusterLabels> clusterLabels(quickUnion.getSize());
    logger.info("Load clusterLabels...");
    entityLabelFile.read(
        reinterpret_cast<char*>(clusterLabels.data()),
        clusterLabels.size() * sizeof(utils::btc::ClusterLabels)
    );
    logger.info(fmt::format("Loaded clusterLabels {} items", clusterLabels.size()));

    logUsedMemory();

    CountList prevAddressCountList(quickUnion.getSize(), 0);
    size_t prevAddressCount = 0;

    CountList prevEntityCountList(quickUnion.getSize(), 0);
    size_t prevEntityCount = 0;

    EntityYearList entityYearList(quickUnion.getSize(), INVALID_ENTITY_YEAR);

    const auto CountPredicator = [](uint8_t value) {
        return value > 0;
    };

    auto groupedDaysList = groupDaysListByYears(daysList, startYear, endYear);
    for (const auto& yearDaysList : groupedDaysList) {
        const auto& year = yearDaysList.first;

        logger.info(fmt::format("\n\n======================== Process year: {} ========================\n", year));

        auto addressOutputFilePath = addressOutputBaseDirPath / (year + ".new");
        auto newAddressOutputFilePath = addressOutputBaseDirPath / (year + ".new.csv");
        auto entityOutputFilePath = entityOutputBaseDirPath / (year + ".new");
        auto newEntityOutputFilePath = entityOutputBaseDirPath / (year + ".new.csv");
        auto activeEntityBinaryOutputFilePath = entityOutputBaseDirPath / (year + ".active");
        auto activeEntityOutputFilePath = entityOutputBaseDirPath / (year + ".active.csv");
        if (fs::exists(addressOutputFilePath) && fs::exists(entityOutputFilePath)) {
            logger.info(fmt::format("Load existed file", year));
            auto loadedCount = loadCountList(addressOutputFilePath.string(), prevAddressCountList);
            prevAddressCount = std::count_if(
                std::execution::par, prevAddressCountList.begin(), prevAddressCountList.end(), CountPredicator
            );
            logger.info(fmt::format("Loaded {} records of year {}", loadedCount, year));
            logUsedMemory();

            logger.info(fmt::format("Load existed file", year));
            loadedCount = loadCountList(entityOutputFilePath.string(), prevEntityCountList);
            prevEntityCount = std::count_if(
                std::execution::par, prevEntityCountList.begin(), prevEntityCountList.end(), CountPredicator
            );
            logger.info(fmt::format("Loaded {} records of year {}", loadedCount, year));
            logUsedMemory();

            logger.info(fmt::format("Loaded address count: {}", prevAddressCount));
            logger.info(fmt::format("Loaded entity count: {}", prevEntityCount));

            int16_t yearValue = static_cast<int16_t>(std::stoi(year));
            logger.info(fmt::format("Calculate entity year for {}", year));
            for (std::size_t addressId = 0; addressId != prevEntityCountList.size(); ++addressId) {
                if (entityYearList[addressId] == INVALID_ENTITY_YEAR && prevEntityCountList[addressId]) {
                    entityYearList[addressId] = yearValue;
                }
            }

            continue;
        }

        const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(
            yearDaysList.second, workerCount
        );
        uint32_t workerIndex = 0;
        std::vector<std::future<void>> tasks;

        CountList currentAddressCountList = prevAddressCountList;
        CountList currentEntityCountList = prevEntityCountList;
        CountList activateEntityCountList(quickUnion.getSize(), 0);

        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(
                std::async(
                    generateAddressStatisticsOfDays,
                    workerIndex,
                    &taskChunk,
                    &quickUnion,
                    &currentAddressCountList,
                    &currentEntityCountList,
                    &activateEntityCountList
                )
            );

            ++workerIndex;
        }

        utils::waitForTasks(logger, tasks);

        size_t currentAddressCount = std::count_if(
            std::execution::par, currentAddressCountList.begin(), currentAddressCountList.end(), CountPredicator
        );
        size_t newAddressCount = currentAddressCount > prevAddressCount ? currentAddressCount - prevAddressCount : 0;

        size_t currentEntityCount = std::count_if(
            std::execution::par, currentEntityCountList.begin(), currentEntityCountList.end(), CountPredicator
        );
        size_t newEntityCount = currentEntityCount > prevEntityCount ? currentEntityCount - prevEntityCount : 0;

        size_t activateEntityCount = std::count_if(
            std::execution::par, activateEntityCountList.begin(), activateEntityCountList.end(), CountPredicator
        );

        dumpNewAddressFile(
            newAddressOutputFilePath.string(),
            id2Address,
            prevAddressCountList,
            currentAddressCountList
        );

        dumpNewEntityFile(
            newEntityOutputFilePath.string(),
            quickUnion,
            clusterLabels,
            prevEntityCountList,
            currentEntityCountList
        );

        dumpActivateEntityFile(
            activeEntityOutputFilePath.string(),
            quickUnion,
            clusterLabels,
            activateEntityCountList
        );

        prevAddressCountList = currentAddressCountList;
        prevAddressCount = currentAddressCount;
        dumpCountList(addressOutputFilePath.string(), prevAddressCountList);

        prevEntityCountList = currentEntityCountList;
        prevEntityCount = currentEntityCount;
        dumpCountList(entityOutputFilePath.string(), prevEntityCountList);

        dumpCountList(activeEntityBinaryOutputFilePath.string(), activateEntityCountList);

        auto summaryOutputFilePath = summaryOutputBaseDirPath / year;
        dumpSummary(summaryOutputFilePath.string(),
            currentAddressCount,
            currentEntityCount,
            newAddressCount,
            newEntityCount,
            activateEntityCount
        );

        int16_t yearValue = static_cast<int16_t>(std::stoi(year));
        logger.info(fmt::format("Calculate entity year for {}", year));
        for (std::size_t addressId = 0; addressId != currentEntityCountList.size(); ++addressId) {
            if (entityYearList[addressId] == INVALID_ENTITY_YEAR && currentEntityCountList[addressId]) {
                entityYearList[addressId] = yearValue;
            }
        }
    }

    fs::path yearOutputFilePath = fs::path(outputBaseDirPath) / "entity-year.out";

    dumpYearList(yearOutputFilePath.string(), entityYearList);

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_gen_address_statistics");

    program.add_argument("days_dir_list")
        .required()
        .help("The list of days dirs");

    program.add_argument("output_base_dir")
        .required()
        .help("The base directory of statistics files");

    program.add_argument("--id2addr")
        .help("address file")
        .required();

    program.add_argument("--union_file")
        .help("Union find file")
        .required();

    program.add_argument("--entity_label_file")
        .help("Entity label file")
        .required();

    program.add_argument("--start_year")
        .help("Start year")
        .scan<'d', uint32_t>()
        .default_value(0u);

    program.add_argument("--end_year")
        .help("End year")
        .scan<'d', uint32_t>()
        .default_value(0u);

    program.add_argument("-w", "--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    return program;
}

std::vector<std::pair<std::string, std::vector<std::string>>>
groupDaysListByYears(const std::vector<std::string>& daysList, uint32_t startYear, uint32_t endYear) {
    std::map<std::string, std::vector<std::string>> yearDaysListMap;
    
    for (const auto& dayDirPathLine : daysList) {
        fs::path dayDirPath(dayDirPathLine);

        const std::string& dirName = dayDirPath.filename().string();
        const auto& year = dirName.substr(0, 4);
        const auto yearValue = std::stoi(year);
        if ((startYear > 0 && yearValue < startYear) || (endYear > 0 && yearValue > endYear)) {
            continue;
        }

        const auto& yearDaysListIt = yearDaysListMap.find(year);
        if (yearDaysListIt == yearDaysListMap.end()) {
            yearDaysListMap[year] = std::vector<std::string>{ dayDirPathLine };
        }
        else {
            yearDaysListIt->second.push_back(dayDirPathLine);
        }
    }

    std::vector<
        std::pair<std::string, std::vector<std::string>>
    > groupedDaysList(yearDaysListMap.begin(), yearDaysListMap.end());

    std::sort(groupedDaysList.begin(), groupedDaysList.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });

    return groupedDaysList;
}

void generateAddressStatisticsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    const utils::btc::WeightedQuickUnion* quickUnion,
    CountList* addressCountList,
    CountList* entityCountList,
    CountList* activateEntityCountList
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    for (const auto& dayDir : *daysDirList) {
        calculateAddressStatisticsOfDays(
            dayDir,
            *quickUnion,
            *addressCountList,
            *entityCountList,
            *activateEntityCountList
        );
    }
}


void calculateAddressStatisticsOfDays(
    const std::string& dayDir,
    const utils::btc::WeightedQuickUnion& quickUnion,
    CountList& addressCountList,
    CountList& entityCountList,
    CountList& activateEntityCountList
) {
    try {
        auto convertedBlocksFilePath = fmt::format("{}/{}", dayDir, "converted-block-list.json");
        logger.info(fmt::format("Process combined blocks file: {}", dayDir));

        std::ifstream convertedBlocksFile(convertedBlocksFilePath.c_str());
        if (!convertedBlocksFile.is_open()) {
            logger.warning(fmt::format("Skip processing blocks by date because file not exists: {}", convertedBlocksFilePath));
            return;
        }

        logUsedMemory();
        json blocks;
        convertedBlocksFile >> blocks;
        logger.info(fmt::format("Block count: {} {}", dayDir, blocks.size()));
        logUsedMemory();

        for (const auto& block : blocks) {
            calculateAddressStatisticsOfBlock(
                block,
                quickUnion,
                addressCountList,
                entityCountList,
                activateEntityCountList
            );
        }

        logUsedMemory();

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

void calculateAddressStatisticsOfBlock(
    const json& block,
    const utils::btc::WeightedQuickUnion& quickUnion,
    CountList& addressCountList,
    CountList& entityCountList,
    CountList& activateEntityCountList
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        const auto& txs = block["tx"];

        for (const auto& tx : txs) {
            calculateAddressStatisticsOfTx(
                tx, quickUnion, addressCountList, entityCountList, activateEntityCountList
            );
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}", blockHash));
        logger.error(e.what());
    }
}

void calculateAddressStatisticsOfTx(
    const json& tx,
    const utils::btc::WeightedQuickUnion& quickUnion,
    CountList& addressCountList,
    CountList& entityCountList,
    CountList& activateEntityCountList
) {
    std::string txHash = utils::json::get(tx, "hash");

    try {
        const auto& inputs = utils::json::get(tx, "inputs");
        for (const auto& input : inputs) {
            const auto prevOutItem = input.find("prev_out");
            if (prevOutItem == input.cend()) {
                continue;
            }
            const auto& prevOut = prevOutItem.value();

            const auto addrItem = prevOut.find("addr");
            if (addrItem == prevOut.cend()) {
                continue;
            }
            BtcId addressId = addrItem.value();
            processAddress(
                addressId,
                quickUnion,
                addressCountList,
                entityCountList,
                activateEntityCountList
            );
        }

        auto& outputs = utils::json::get(tx, "out");
        for (auto& output : outputs) {
            auto addrItem = output.find("addr");
            if (addrItem == output.cend()) {
                continue;
            }
            BtcId addressId = addrItem.value();
            processAddress(addressId, quickUnion, addressCountList, entityCountList, activateEntityCountList);
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}", txHash));
        logger.error(e.what());
    }
}

void processAddress(
    BtcId addressId,
    const utils::btc::WeightedQuickUnion& quickUnion,
    CountList& addressCountList,
    CountList& entityCountList,
    CountList& activateEntityCountList
) {
    addressCountList[addressId] = 1;

    auto entityId = quickUnion.findRoot(addressId);
    entityCountList[entityId] = 1;
    activateEntityCountList[entityId] = 1;
}

std::size_t loadCountList(
    const std::string& inputFilePath,
    CountList& countList
) {
    logger.info(fmt::format("Load count from: {}", inputFilePath));
    std::ifstream inputFile(inputFilePath.c_str(), std::ios::binary);
    std::size_t loadedCount = 0;

    inputFile.read(reinterpret_cast<char*>(&loadedCount), sizeof(loadedCount));
    inputFile.read(reinterpret_cast<char*>(countList.data()), countList.size());

    return loadedCount;
}

void dumpCountList(
    const std::string& outputFilePath,
    CountList& countList
) {
    logger.info(fmt::format("Dump count to: {}", outputFilePath));

    std::ofstream outputFile(outputFilePath.c_str(), std::ios::binary);

    std::size_t countSize = countList.size();
    outputFile.write(reinterpret_cast<char*>(&countSize), sizeof(countSize));
    outputFile.write(reinterpret_cast<const char*>(countList.data()), countSize);
}

void dumpYearList(
    const std::string& outputFilePath,
    EntityYearList& yearList
) {
    logger.info(fmt::format("Dump entity year to: {}", outputFilePath));

    std::ofstream outputFile(outputFilePath.c_str(), std::ios::binary);

    std::size_t yearSize = yearList.size();
    outputFile.write(reinterpret_cast<char*>(&yearSize), sizeof(yearSize));
    outputFile.write(reinterpret_cast<const char*>(yearList.data()), yearSize * sizeof(int16_t));
}

void dumpSummary(
    const std::string& outputFilePath,
    size_t allAddressCount,
    size_t allEntityCount,
    size_t newAddressCount,
    size_t newEntityCount,
    size_t activateEntityCount
) {
    logger.info(fmt::format("Dump summary to: {}", outputFilePath));

    std::ofstream outputFile(outputFilePath.c_str());
    outputFile << fmt::format("All addresses: {}\n", allAddressCount);
    outputFile << fmt::format("All entities: {}\n", allEntityCount);
    outputFile << fmt::format("New addresses: {}\n", newAddressCount);
    outputFile << fmt::format("New entities: {}\n", newEntityCount);
    outputFile << fmt::format("Activate entities: {}\n", activateEntityCount);
}

void dumpNewAddressFile(
    const std::string& outputFilePath,
    const std::vector<std::string>& id2address,
    const CountList& prevCountList,
    const CountList& currCountList
) {
    logger.info(fmt::format("Dump new address file to: {}", outputFilePath));
    std::ofstream newAddressFile(outputFilePath.c_str());

    BtcId addressIdCount = static_cast<BtcId>(id2address.size());
    for (BtcId addressId = 0; addressId != addressIdCount; ++addressId) {
        if (!prevCountList[addressId] && currCountList[addressId]) {
            newAddressFile << id2address[addressId] << std::endl;
        }
    }
}

void dumpNewEntityFile(
    const std::string& outputFilePath,
    utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const CountList& prevCountList,
    const CountList& currCountList
) {
    logger.info(fmt::format("Dump new entity file to: {}", outputFilePath));
    std::ofstream newEntityFile(outputFilePath.c_str());

    utils::btc::WeightedQuickUnionClusters quickUnionClusters(quickUnion);
    quickUnionClusters.forEach(
        [&newEntityFile, &clusterLabels, &prevCountList, &currCountList](BtcId entityId, BtcId btcSize) -> void {
            if (!prevCountList[entityId] && currCountList[entityId]) {
                utils::btc::ClusterLabels clusterLabel = clusterLabels[entityId];
                bool isMiner = clusterLabel.isMiner;
                bool isLabeldExchange = clusterLabel.isLabeldExchange;
                bool isFoundExchange = clusterLabel.isFoundExchange;

                newEntityFile << fmt::format("{},{},{},{}",
                    entityId, isMiner, isLabeldExchange, isFoundExchange
                ) << std::endl;
            }
        }
    );
}

void dumpActivateEntityFile(
    const std::string& outputFilePath,
    utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const CountList& activateEntityCountList
) {
    logger.info(fmt::format("Dump activate entity file to: {}", outputFilePath));
    std::ofstream newEntityFile(outputFilePath.c_str());

    utils::btc::WeightedQuickUnionClusters quickUnionClusters(quickUnion);
    quickUnionClusters.forEach(
        [&newEntityFile, &clusterLabels, &activateEntityCountList](BtcId entityId, BtcId btcSize) -> void {
            if (activateEntityCountList[entityId]) {
                utils::btc::ClusterLabels clusterLabel = clusterLabels[entityId];
                bool isMiner = clusterLabel.isMiner;
                bool isLabeldExchange = clusterLabel.isLabeldExchange;
                bool isFoundExchange = clusterLabel.isFoundExchange;

                newEntityFile << fmt::format("{},{},{},{}",
                    entityId, isMiner, isLabeldExchange, isFoundExchange
                ) << std::endl;
            }
        }
    );
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}