// 计算实体余额

#include "btc-config.h"
#include "final_export_entity_month_balance/logger.h"

#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "fmt/format.h"
#include <argparse/argparse.hpp>
#include <nlohmann/json.hpp>

#include <cstdlib>
#include <iostream>
#include <thread>
#include <filesystem>
#include <iomanip>

namespace fs = std::filesystem;
using json = nlohmann::json;
using BalanceValue = double;
using BalanceList = std::vector<BalanceValue>;
using BalanceListPtr = std::shared_ptr<BalanceList>;
using EntityYearList = std::vector<int16_t>;
using SortedBalanceItem = std::pair<std::size_t, BalanceValue>;
using SortedBalanceList = std::vector<SortedBalanceItem>;
using CountList = std::vector<uint8_t>;

struct AverageFilterOption {
    std::string key = "default";
    double removeLowValue = 0.0;
    bool removeMiner = false;
    bool removeLabeledExchange = false;
    bool removeFoundExchange = false;
    double removeTopPercents = 0.0;
    bool onlyLongTerm = false;
    bool exportHighest = false;

    static AverageFilterOption FromJsonObject(json jsonObject) {
        AverageFilterOption options{
            .key = utils::json::get(jsonObject, "key"),
            .removeLowValue = utils::json::get(jsonObject, "removeLowValue"),
            .removeMiner = utils::json::get(jsonObject, "removeMiner"),
            .removeLabeledExchange = utils::json::get(jsonObject, "removeLabeledExchange"),
            .removeFoundExchange = utils::json::get(jsonObject, "removeFoundExchange"),
            .removeTopPercents = utils::json::get(jsonObject, "removeTopPercents"),
            .onlyLongTerm = utils::json::get(jsonObject, "onlyLongTerm"),
            .exportHighest = utils::json::get(jsonObject, "exportHighest"),
        };

        return options;
    }

    static std::vector<AverageFilterOption> FromJsonArray(json jsonArray) {
        std::vector<AverageFilterOption> optionsArray;

        for (const json& jsonObject : jsonArray) {
            const auto& options = FromJsonObject(jsonObject);

            optionsArray.push_back(options);
        }

        return optionsArray;
    }
};

inline void logUsedMemory();

auto& logger = getLogger();

static argparse::ArgumentParser createArgumentParser();

std::vector<std::pair<std::string, std::vector<std::string>>>
groupDaysListByYearMonths(const std::vector<std::string>& daysList, uint32_t year);

BalanceListPtr generateBalanceListOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    BtcId maxSize
);

void calculateBalanceListOfDays(
    const std::string& dayDir,
    BalanceListPtr balanceList
);

void calculateBalanceListOfBlock(
    const json& block,
    BalanceListPtr balanceList
);

void calculateBalanceListOfTx(
    const json& tx,
    BalanceListPtr balanceList
);

void mergeBalanceList(
    BalanceList& dest,
    const BalanceList& src
);

std::set<BtcId> loadExcludeRootAddresses(
    const std::string& excludeAddressListFilePath,
    const utils::btc::WeightedQuickUnion& quickUnion
);

BalanceList processYearMonthAddressBalance(
    CountList& entityCountList,
    CountList& activeEntityCountList,
    const BalanceList& balanceList,
    utils::btc::WeightedQuickUnion& quickUnion,
    const std::set<BtcId>& exchangeRootAddresseIds
);

void processYearMonthEntityBalance(
    uint32_t year,
    const BalanceList& balanceList,
    const CountList& entityCountList,
    const CountList& activeEntityList,
    const fs::path& entityBalanceFilesDir,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    std::size_t initialBufferSize,
    std::uint32_t distributionSegment,
    const std::vector<AverageFilterOption>& averageFilterOptions
);

void generateEntityAverageBalance(
    const std::string& entityAverageFilePath,
    const SortedBalanceList& balanceList,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels
);

void generateRichestEntites(
    const std::string& richestEntitiesFilePath,
    const EntityYearList& entityYearList,
    const SortedBalanceList& balanceList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels
);

SortedBalanceList sortBalanceList(
    uint32_t year,
    const BalanceList& balanceList,
    const CountList& entityCountList,
    const CountList& activeEntityList,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    std::size_t initialEntityCount,
    const AverageFilterOption& averageFilterOptions
);

bool isToRemoveEntity(
    BalanceValue value,
    uint16_t currentYear,
    uint16_t entityYear,
    uint8_t isActive,
    const utils::btc::ClusterLabels clusterLabel,
    const AverageFilterOption& averageFilterOptions
);

std::size_t loadBalanceList(
    const std::string& inputFilePath,
    BalanceList& balanceList
);
std::size_t loadYearList(
    const std::string& inputFilePath,
    EntityYearList& yearList
);
std::size_t loadCountList(
    const std::string& inputFilePath,
    CountList& countList
);

int compareBalanceItem(const void* x, const void* y) {
    const SortedBalanceItem lhs = *static_cast<const SortedBalanceItem*>(x);
    const SortedBalanceItem rhs = *static_cast<const SortedBalanceItem*>(y);

    if (rhs.second > lhs.second) {
        return 1;
    }
    else if (rhs.second < lhs.second) {
        return -1;
    }

    return 0;
}

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

    try {
        // 读取UnionFind文件
        const std::string ufFilePath = argumentParser.get("--union_file");
        utils::btc::WeightedQuickUnion quickUnion(1);
        logger.info(fmt::format("Load quickUnion from {}", ufFilePath));
        quickUnion.load(ufFilePath);
        logger.info(fmt::format("Loaded quickUnion {} items from {}", quickUnion.getSize(), ufFilePath));
        auto maxId = quickUnion.getSize();

        logUsedMemory();

        // 读取实体标签文件
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

        // 加载需要排除的实体地址文件
        const std::string excludeAddressListFilePath = argumentParser.get("--exclude_addrs");
        const std::set<BtcId>& excludeRootAddresses = loadExcludeRootAddresses(excludeAddressListFilePath, quickUnion);

        logUsedMemory();

        // 初始化原始余额列表、有效实体列表
        std::string balanceBaseDirPath = argumentParser.get("--balance_base_dir");
        BalanceList addressBalanceList(quickUnion.getSize(), 0.0);
        CountList entityCountList(quickUnion.getSize(), 0);

        logUsedMemory();

        // 初始化实体年份列表
        std::string addressReportDirPath = argumentParser.get("--address_report_dir");
        fs::path entityYearFilePath = fs::path(addressReportDirPath) / "entity-year.out";
        EntityYearList entityYearList;
        std::size_t loadedEntityYears = loadYearList(entityYearFilePath.string(), entityYearList);
        logger.info(fmt::format("Loaded entity years: {}", loadedEntityYears));

        logUsedMemory();

        // 获取年份
        auto year = argumentParser.get<uint32_t>("--year");
        logger.info(fmt::format("Using year: {}", year));
        if (year > 2009) {
            uint32_t previousYear = year - 1;

            // 加载上一年的余额列表
            auto previousYearBalanceFilePath = fs::path(balanceBaseDirPath) / fmt::format("{}.out", previousYear);
            loadBalanceList(previousYearBalanceFilePath.string(), addressBalanceList);

            // 加载上一年的有效实体列表
            auto entityCountListFilePath = fs::path(addressReportDirPath)
                / "entity" / fmt::format("{}.new", previousYear);
            loadCountList(entityCountListFilePath.string(), entityCountList);
        }

        logUsedMemory();

        // 按月生成文件列表
        std::string daysListFilePath = argumentParser.get("days_dir_list");
        logger.info(fmt::format("Read tasks form {}", daysListFilePath));
        std::vector<std::string> daysList = utils::readLines(daysListFilePath);
        logger.info(fmt::format("Read tasks count: {}", daysList.size()));
        auto groupedDaysList = groupDaysListByYearMonths(daysList, year);

        // 计算线程数量
        uint32_t workerCount = std::min(argumentParser.get<uint32_t>("--worker_count"), std::thread::hardware_concurrency());
        logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
        logger.info(fmt::format("Worker count: {}", workerCount));

        // 获取统计参数
        auto initialBufferSize = argumentParser.get<std::size_t>("--buffer_size");
        logger.info(fmt::format("Using initial balance buffer size: {}", initialBufferSize));

        auto distributionSegment = argumentParser.get<uint32_t>("--distribution_segments");
        logger.info(fmt::format("Using end distribution segment: {}", distributionSegment));

        // 获取局部统计选项
        std::vector<AverageFilterOption> averageFilterOptions(1);
        std::string filterOptionsFilePath = argumentParser.get("--filter_options");
        if (!filterOptionsFilePath.empty()) {
            std::ifstream filterOptionsFile(filterOptionsFilePath.c_str());
            if (!filterOptionsFile.is_open()) {
                logger.warning(fmt::format("Can not open filter options file: {}", filterOptionsFilePath));
            }
            else {
                logger.info(fmt::format("Load filter options file: {}", filterOptionsFilePath));
                json filterOptionsJson;
                filterOptionsFile >> filterOptionsJson;

                averageFilterOptions = AverageFilterOption::FromJsonArray(filterOptionsJson);
            }
        }

        std::string outputBaseDirPath = argumentParser.get("output_base_dir");
        const auto entityBalanceFileBaseDirPath = fs::path(outputBaseDirPath) / "months" / std::to_string(year);

        // 逐月计算
        for (const auto& groupedDays : groupedDaysList) {
            // 第一步：计算地址余额
            std::string month = groupedDays.first;
            const auto& daysList = groupedDays.second;

            logger.info(fmt::format("\n\n==================== Process month: {}-{} ====================\n", year, month));
            const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(
                daysList, workerCount
            );

            std::vector<std::future<BalanceListPtr>> tasks;
            uint32_t workerIndex = 0;
            for (const auto& taskChunk : taskChunks) {
                tasks.push_back(
                    std::async(generateBalanceListOfDays, workerIndex, &taskChunk, maxId)
                );

                ++workerIndex;
            }

            logger.info("Merge balance lists");
            for (auto& task : tasks) {
                BalanceListPtr taskResult = std::move(task.get());
                mergeBalanceList(addressBalanceList, *taskResult);

                taskResult.reset();
            }

            // 第一步：计算实体余额
            // TODO: 计算entityCountList
            // TODO: 计算activateCountList
            CountList entityActiveCountList(quickUnion.getSize(), 0);
            const auto& entityBalanceList = processYearMonthAddressBalance(
                entityCountList,
                entityActiveCountList,
                addressBalanceList,
                quickUnion,
                excludeRootAddresses
            );

            auto entityBalanceFilesDir = entityBalanceFileBaseDirPath / month;
            if (!fs::exists(entityBalanceFilesDir)) {
                fs::create_directories(entityBalanceFilesDir);
            }
            processYearMonthEntityBalance(
                year,
                entityBalanceList,
                entityCountList,
                entityActiveCountList,
                entityBalanceFilesDir,
                entityYearList,
                clusterLabels,
                initialBufferSize,
                distributionSegment,
                averageFilterOptions
            );
        }
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_filter_address_balance");

    program.add_argument("days_dir_list")
        .required()
        .help("The days dir list file path");

    program.add_argument("output_base_dir")
        .required()
        .help("The base directory of entity balance list files");

    program.add_argument("--balance_base_dir")
        .required()
        .help("The base directory of address balance list files");

    program.add_argument("--address_report_dir")
        .required()
        .help("The base directory of address report files");

    program.add_argument("--filter_options")
        .default_value("")
        .help("The file path of filter options");

    program.add_argument("--union_file")
        .help("Union find file")
        .required();

    program.add_argument("--entity_label_file")
        .help("Entity label file")
        .required();

    program.add_argument("-e", "--exclude_addrs")
        .help("Exclude addresses file path")
        .default_value("");

    program.add_argument("--year")
        .help("Start year")
        .scan<'d', uint32_t>()
        .required();

    program.add_argument("-w", "--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    program.add_argument("--buffer_size")
        .help("Balance buffer initial size")
        .scan<'d', std::size_t>()
        .default_value(4096u);

    program.add_argument("--distribution_segments")
        .help("Distribution segments")
        .scan<'d', std::uint32_t>()
        .default_value(100u);

    return program;
}

std::vector<std::pair<std::string, std::vector<std::string>>>
groupDaysListByYearMonths(const std::vector<std::string>& daysList, uint32_t year) {
    std::string selectedYearStr = std::to_string(year);

    std::map<std::string, std::vector<std::string>> monthDaysListMap;

    for (const auto& dayDirPathLine : daysList) {
        fs::path dayDirPath(dayDirPathLine);

        const std::string& dirName = dayDirPath.filename().string();
        const auto& year = dirName.substr(0, 4);
        if (year != selectedYearStr) {
            continue;
        }

        const auto& month = dirName.substr(5, 2);
        const auto& monthDaysListIt = monthDaysListMap.find(month);
        if (monthDaysListIt == monthDaysListMap.end()) {
            monthDaysListMap[month] = std::vector<std::string>{ dayDirPathLine };
        }
        else {
            monthDaysListIt->second.push_back(dayDirPathLine);
        }
    }

    std::vector<
        std::pair<std::string, std::vector<std::string>>
    > groupedDaysList(monthDaysListMap.begin(), monthDaysListMap.end());

    std::sort(groupedDaysList.begin(), groupedDaysList.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });

    return groupedDaysList;
}


BalanceListPtr generateBalanceListOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    BtcId maxId
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    BalanceListPtr balanceList = std::make_shared<BalanceList>(maxId, 0.0);
    for (const auto& dayDir : *daysDirList) {
        calculateBalanceListOfDays(dayDir, balanceList);
    }

    return balanceList;
}

void calculateBalanceListOfDays(
    const std::string& dayDir,
    BalanceListPtr balanceList
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
            calculateBalanceListOfBlock(block, balanceList);
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

void calculateBalanceListOfBlock(
    const json& block,
    BalanceListPtr balanceList
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        const auto& txs = block["tx"];

        for (const auto& tx : txs) {
            calculateBalanceListOfTx(tx, balanceList);
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}", blockHash));
        logger.error(e.what());
    }
}

void calculateBalanceListOfTx(
    const json& tx,
    BalanceListPtr balanceList
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

            const auto valueItem = prevOut.find("value");
            if (valueItem == prevOut.cend()) {
                continue;
            }
            BalanceValue value = valueItem.value();

            balanceList->at(addressId) -= value;
        }

        auto& outputs = utils::json::get(tx, "out");
        for (auto& output : outputs) {
            auto addrItem = output.find("addr");
            if (addrItem == output.cend()) {
                continue;
            }
            BtcId addressId = addrItem.value();

            const auto valueItem = output.find("value");
            if (valueItem == output.cend()) {
                continue;
            }
            BalanceValue value = valueItem.value();

            balanceList->at(addressId) += value;
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}", txHash));
        logger.error(e.what());
    }
}

void mergeBalanceList(
    BalanceList& dest,
    const BalanceList& src
) {
    auto destIt = dest.begin();
    auto srcIt = src.begin();

    while (destIt != dest.end() && srcIt != dest.end()) {
        *destIt += *srcIt;

        ++destIt;
        ++srcIt;
    }
}

std::set<BtcId> loadExcludeRootAddresses(
    const std::string& excludeAddressListFilePath,
    const utils::btc::WeightedQuickUnion& quickUnion
) {
    std::set<BtcId> excludeRootAddresses;

    if (!excludeAddressListFilePath.empty()) {
        logger.info(fmt::format("Load excludeAddresses: {}", excludeAddressListFilePath));
        std::vector<BtcId> excludeAddresses = utils::readLines<std::vector<BtcId>, BtcId>(
            excludeAddressListFilePath,
            [](const std::string& line) -> BtcId {
                return std::stoi(line);
        });
        logger.info(fmt::format("Loaded excludeAddresses: {}", excludeAddresses.size()));

        for (BtcId addressId : excludeAddresses) {
            excludeRootAddresses.insert(quickUnion.findRoot(addressId));
        }
    }

    return excludeRootAddresses;
}

BalanceList processYearMonthAddressBalance(
    CountList& entityCountList,
    CountList& activeEntityCountList,
    const BalanceList& balanceList,
    utils::btc::WeightedQuickUnion& quickUnion,
    const std::set<BtcId>& exchangeRootAddresseIds
) {
    using utils::btc::BtcSize;

    BalanceList clusterBalances(balanceList.size(), 0.0);
    BtcId currentAddressId = 0;
    logger.info("Calculate balance list of entities");
    for (auto balance : balanceList) {
        BtcId entityId = quickUnion.findRoot(currentAddressId);

        if (entityId >= balanceList.size()) {
            logger.error(fmt::format("Entity id {} is greater than balance array size: {}", entityId, balanceList.size()));
            continue;
        }

        if (exchangeRootAddresseIds.contains(entityId)) {
            continue;
        }

        clusterBalances.at(entityId) += std::max(0.0, balance);
        entityCountList.at(entityId) = 1;
        activeEntityCountList.at(entityId) = 1;

        ++currentAddressId;
    }

    return clusterBalances;
}

void processYearMonthEntityBalance(
    uint32_t year,
    const BalanceList& balanceList,
    const CountList& entityCountList,
    const CountList& activeEntityList,
    const fs::path& entityBalanceFilesDir,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    std::size_t initialBufferSize,
    std::uint32_t distributionSegment,
    const std::vector<AverageFilterOption>& averageFilterOptions
) {
    using utils::btc::BtcSize;

    std::size_t zeroEntityCount = std::count_if(balanceList.begin(), balanceList.end(), [](BalanceValue value) {
        return value <= 0;
    });

    logUsedMemory();
    std::size_t nonzeroEntityCount = balanceList.size() - zeroEntityCount;

    for (const AverageFilterOption& averageFilterOption : averageFilterOptions) {
        const auto& sortedBalanceList = sortBalanceList(
            year,
            balanceList,
            entityCountList,
            activeEntityList,
            entityYearList,
            clusterLabels,
            nonzeroEntityCount,
            averageFilterOption
        );
        logUsedMemory();

        fs::path avgFilePath = entityBalanceFilesDir / fmt::format("{}.avgs", averageFilterOption.key);
        generateEntityAverageBalance(
            avgFilePath.string(),
            sortedBalanceList,
            entityYearList,
            clusterLabels
        );

        fs::path richestFilePath = entityBalanceFilesDir / fmt::format("{}.richest", averageFilterOption.key);
        if (averageFilterOption.exportHighest) {
            generateRichestEntites(
                richestFilePath.string(),
                entityYearList,
                sortedBalanceList,
                clusterLabels
            );
        }
    }
}

void generateEntityAverageBalance(
    const std::string& entityAverageFilePath,
    const SortedBalanceList& balanceList,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels
) {
    logger.info("Generate entity average balance");

    double totalBalance = 0;
    for (const auto& balanceItem : balanceList) {
        totalBalance += balanceItem.second;
    }

    double averageBalance = totalBalance / balanceList.size();
    std::ofstream outputFile(entityAverageFilePath.c_str());
    logger.info(fmt::format("Output all average balance {} to {}", averageBalance, entityAverageFilePath));
    outputFile << fmt::format("All average: {:.19g}", averageBalance) << std::endl;

    outputFile << "Average balance of segments" << std::endl;
    outputFile << "BeginPercent,EndPercent,BeginRank,EndRank,AvgBalance,MinBalance,MaxBalance" << std::endl;

    for (std::size_t startPercent = 0, startIndex = 0;
        startPercent != 100;
        ++startPercent
        ) {
        auto endPercent = startPercent + 1;
        auto endIndex = static_cast<std::size_t>(
            std::ceil(static_cast<double>(endPercent) * balanceList.size() / 100)
            );
        if (startPercent == 99) {
            endIndex = balanceList.size();
        }

        double segmentTotalBalance = 0;
        for (auto currentIndex = startIndex; currentIndex != endIndex; ++currentIndex) {
            segmentTotalBalance += balanceList[currentIndex].second;
        }
        double segmentEntityCount = endIndex - startIndex;
        double segmentAverageBalance = segmentTotalBalance / segmentEntityCount;

        double minBalance = balanceList[startIndex].second;
        double maxBalance = balanceList[endIndex - 1].second;
        outputFile << fmt::format(
            "{},{},{},{},{:.19g},{:.19g},{:.19g}",
            startPercent, startPercent + 1, startIndex, endIndex,
            segmentAverageBalance, minBalance, maxBalance
        ) << std::endl;

        startIndex = endIndex;
    }
}

void generateRichestEntites(
    const std::string& richestEntitiesFilePath,
    const EntityYearList& entityYearList,
    const SortedBalanceList& balanceList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels
) {
    logger.info("Generate riches entities");

    std::ofstream outputFile(richestEntitiesFilePath.c_str());
    logger.info(fmt::format("Output richest entities balance to {}", richestEntitiesFilePath));
    outputFile <<
        "Rank,Entity,Balance,Year,isMiner,isLabeldExchange,isFoundExchange" <<
        std::endl;

    std::size_t maxEntityCount = std::min(balanceList.size(), static_cast<std::size_t>(10000));
    for (uint32_t entityRank = 0; entityRank != maxEntityCount; ++entityRank) {
        const auto& entityBalanceItem = balanceList[entityRank];
        auto entityId = entityBalanceItem.first;

        utils::btc::ClusterLabels clusterLabel = clusterLabels[entityId];
        bool isMiner = clusterLabel.isMiner;
        bool isLabeldExchange = clusterLabel.isLabeldExchange;
        bool isFoundExchange = clusterLabel.isFoundExchange;

        outputFile << fmt::format(
            "{},{},{:.19g},{},{},{},{}",
            entityRank + 1, entityId, entityBalanceItem.second, entityYearList[entityId],
            isMiner, isLabeldExchange, isFoundExchange
        ) << std::endl;
    }
}


SortedBalanceList sortBalanceList(
    uint32_t year,
    const BalanceList& balanceList,
    const CountList& entityCountList,
    const CountList& activeEntityList,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    std::size_t initialEntityCount,
    const AverageFilterOption& averageFilterOptions
) {
    logger.info(fmt::format("Sort balance list with initial count: {}", initialEntityCount));

    SortedBalanceList sortedBalanceList;
    sortedBalanceList.reserve(initialEntityCount);
    logUsedMemory();

    size_t addressId = 0;
    for (auto countValue : entityCountList) {
        // 此处会删除余额为0的实体
        if (countValue && !isToRemoveEntity(
            balanceList[addressId],
            year,
            entityYearList[addressId],
            activeEntityList[addressId],
            clusterLabels[addressId],
            averageFilterOptions)
            ) {
            sortedBalanceList.push_back(std::make_pair(addressId, balanceList[addressId]));
        }

        ++addressId;
    }
    logUsedMemory();

    logger.info(fmt::format("Begin to sort entities: {}", sortedBalanceList.size()));
    qsort(sortedBalanceList.data(), sortedBalanceList.size(), sizeof(SortedBalanceItem), compareBalanceItem);
    logger.info(fmt::format("Finished sort entities: {}", sortedBalanceList.size()));

    auto removeTopPercents = averageFilterOptions.removeTopPercents;
    if (removeTopPercents > 0) {
        size_t topEntityCount = static_cast<size_t>(
            sortedBalanceList.size() * averageFilterOptions.removeTopPercents / 100
            );
        logger.info(fmt::format("Remove top entities: {}% -> {}", removeTopPercents, topEntityCount));
        sortedBalanceList.erase(sortedBalanceList.begin(), sortedBalanceList.begin() + topEntityCount);
    }

    return sortedBalanceList;
}

bool isToRemoveEntity(
    BalanceValue value,
    uint16_t currentYear,
    uint16_t entityYear,
    uint8_t isActive,
    const utils::btc::ClusterLabels clusterLabel,
    const AverageFilterOption& averageFilterOptions
) {
    bool onlyLongTerm = averageFilterOptions.onlyLongTerm;
    if (onlyLongTerm && (!isActive || entityYear >= currentYear)) {
        return true;
    }

    // 默认为0，会移除所有余额为0的实体
    double removeLowValue = averageFilterOptions.removeLowValue;
    if (value <= removeLowValue) {
        return true;
    }

    bool removeMiner = averageFilterOptions.removeMiner;
    if (removeMiner && clusterLabel.isMiner) {
        return true;
    }
    bool removeLabeledExchange = averageFilterOptions.removeLabeledExchange;
    if (removeLabeledExchange && clusterLabel.isLabeldExchange) {
        return true;
    }
    bool removeFoundExchange = averageFilterOptions.removeFoundExchange;
    if (removeFoundExchange && clusterLabel.isFoundExchange) {
        return true;
    }

    return false;
}

std::size_t loadBalanceList(
    const std::string& inputFilePath,
    BalanceList& balanceList
) {
    logger.info(fmt::format("Load balance from: {}", inputFilePath));
    std::ifstream inputFile(inputFilePath.c_str(), std::ios::binary);

    std::size_t balanceSize = 0;
    inputFile.read(reinterpret_cast<char*>(&balanceSize), sizeof(balanceSize));
    balanceList.resize(balanceSize);
    inputFile.read(reinterpret_cast<char*>(balanceList.data()), sizeof(BalanceValue) * balanceSize);
    logger.info(fmt::format("Loaded {} balance from: {}", balanceSize, inputFilePath));

    return balanceSize;
}

std::size_t loadYearList(
    const std::string& inputFilePath,
    EntityYearList& yearList
) {
    logger.info(fmt::format("Load count from: {}", inputFilePath));
    std::ifstream inputFile(inputFilePath.c_str(), std::ios::binary);
    std::size_t loadedCount = 0;

    inputFile.read(reinterpret_cast<char*>(&loadedCount), sizeof(loadedCount));
    yearList.resize(loadedCount);
    inputFile.read(reinterpret_cast<char*>(yearList.data()), yearList.size() * sizeof(int16_t));

    return loadedCount;
}

std::size_t loadCountList(
    const std::string& inputFilePath,
    CountList& countList
) {
    logger.info(fmt::format("Load count from: {}", inputFilePath));
    std::ifstream inputFile(inputFilePath.c_str(), std::ios::binary);
    std::size_t loadedCount = 0;

    inputFile.read(reinterpret_cast<char*>(&loadedCount), sizeof(loadedCount));
    countList.resize(loadedCount);
    inputFile.read(reinterpret_cast<char*>(countList.data()), countList.size());
    logger.info(fmt::format("Loaded {} count from: {}", countList.size(), inputFilePath));

    return loadedCount;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}