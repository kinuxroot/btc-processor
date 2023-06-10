// 生成地址的交易数量

#include "btc-config.h"
#include "final_export_tx_counts/logger.h"

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
using TxCountsList = std::vector<std::pair<uint64_t, uint64_t>>;

const auto INVALID_ENTITY_YEAR = std::numeric_limits<int16_t>::min();

namespace fs = std::filesystem;

inline BtcId parseMaxId(const char* maxIdArg);

std::vector<
    std::pair<std::string, std::vector<std::string>>
> groupDaysListByYears(const std::vector<std::string>& daysList, uint32_t startYear, uint32_t endYear);

static argparse::ArgumentParser createArgumentParser();

std::unique_ptr<TxCountsList> generateAddressStatisticsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    utils::btc::WeightedQuickUnion* quickUnion
);

void calculateAddressStatisticsOfDays(
    const std::string& dayDir,
    TxCountsList* txCountsList,
    const utils::btc::WeightedQuickUnion& quickUnion
);

void calculateAddressStatisticsOfBlock(
    const json& block,
    TxCountsList* txCountsList,
    const utils::btc::WeightedQuickUnion& quickUnion
);

void calculateAddressStatisticsOfTx(
    const json& tx,
    TxCountsList* txCountsList,
    const utils::btc::WeightedQuickUnion& quickUnion
);

void dumpCountList(
    const std::string& outputFilePath,
    TxCountsList& countList
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

    std::string outputFilePath = argumentParser.get("output_file");

    uint32_t workerCount = std::min(
        argumentParser.get<uint32_t>("--worker_count"),
        std::thread::hardware_concurrency()
    );
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    logUsedMemory();

    const std::string ufFilePath = argumentParser.get("--union_file");
    utils::btc::WeightedQuickUnion quickUnion(1);
    logger.info(fmt::format("Load quickUnion from {}", ufFilePath));
    quickUnion.load(ufFilePath);
    logger.info(fmt::format("Loaded quickUnion {} items from {}", quickUnion.getSize(), ufFilePath));

    BtcId addressCount = quickUnion.getSize();

    logUsedMemory();

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);
    uint32_t workerIndex = 0;
    std::vector<std::future<std::unique_ptr<TxCountsList>>> tasks;
    for (const auto& taskChunk : taskChunks) {
        tasks.push_back(
            std::async(
                generateAddressStatisticsOfDays,
                workerIndex,
                &taskChunk,
                &quickUnion
            )
        );

        ++workerIndex;
    }

    logger.info("Try to merge tx counts list");
    TxCountsList mergedTxCountsList(addressCount, std::make_pair(0, 0));
    workerIndex = 0;
    for (auto& task : tasks) {
        logger.info(fmt::format("Try to get tx counts list: {}", workerIndex));
        std::unique_ptr<TxCountsList> txCountsListPtr = task.get();
        logger.info(fmt::format("Merge tx counts list: {}", workerIndex));
        TxCountsList& txCountsList = *(txCountsListPtr.get());
        for (BtcId addressId = 0; addressId != addressCount; ++addressId) {
            mergedTxCountsList[addressId].first += txCountsList[addressId].first;
            mergedTxCountsList[addressId].second += txCountsList[addressId].second;
        }
        logger.info(fmt::format("Merged tx counts list: {}", workerIndex));

        ++workerIndex;
    }

    dumpCountList(outputFilePath, mergedTxCountsList);

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_gen_address_statistics");

    program.add_argument("days_dir_list")
        .required()
        .help("The list of days dirs");

    program.add_argument("output_file")
        .required()
        .help("The output file path");

    program.add_argument("--union_file")
        .help("Union find file")
        .required();

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

std::unique_ptr<TxCountsList> generateAddressStatisticsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    utils::btc::WeightedQuickUnion* quickUnion
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));
    BtcId addressCount = quickUnion->getSize();
    std::unique_ptr<TxCountsList> txCountsList = std::make_unique<TxCountsList>(addressCount, std::make_pair(0, 0));

    for (const auto& dayDir : *daysDirList) {
        calculateAddressStatisticsOfDays(
            dayDir,
            txCountsList.get(),
            *quickUnion
        );
    }

    return txCountsList;
}


void calculateAddressStatisticsOfDays(
    const std::string& dayDir,
    TxCountsList* txCountsList,
    const utils::btc::WeightedQuickUnion& quickUnion
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
                txCountsList,
                quickUnion
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
    TxCountsList* txCountsList,
    const utils::btc::WeightedQuickUnion& quickUnion
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        const auto& txs = block["tx"];

        for (const auto& tx : txs) {
            calculateAddressStatisticsOfTx(
                tx, txCountsList, quickUnion
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
    TxCountsList* txCountsList,
    const utils::btc::WeightedQuickUnion& quickUnion
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
            BtcId clusterId = quickUnion.findRoot(addressId);
            ++ txCountsList->at(clusterId).first;
        }

        auto& outputs = utils::json::get(tx, "out");
        for (auto& output : outputs) {
            auto addrItem = output.find("addr");
            if (addrItem == output.cend()) {
                continue;
            }
            BtcId addressId = addrItem.value();
            BtcId clusterId = quickUnion.findRoot(addressId);
            ++txCountsList->at(clusterId).second;
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}", txHash));
        logger.error(e.what());
    }
}

void dumpCountList(
    const std::string& outputFilePath,
    TxCountsList& countList
) {
    logger.info(fmt::format("Dump count to: {}", outputFilePath));

    std::ofstream outputFile(outputFilePath.c_str(), std::ios::binary);

    std::size_t countSize = countList.size();
    outputFile.write(reinterpret_cast<char*>(&countSize), sizeof(countSize));
    outputFile.write(reinterpret_cast<const char*>(countList.data()), countSize * sizeof(countList[0]));
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}