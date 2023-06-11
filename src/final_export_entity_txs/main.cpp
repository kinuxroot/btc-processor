// 生成每个用户相关交易数量、每笔交易的金额、区块编号、手续费

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

void generateEntityTxsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    const utils::btc::WeightedQuickUnion* quickUnion,
    const std::vector<utils::btc::ClusterLabels>* clusterLabels,
    const TxCountsList* txCountsList,
    std::osyncstream* outputFile
);

void calculateAddressStatisticsOfDays(
    uint32_t workerIndex,
    const std::string& dayDir,
    const utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const TxCountsList& txCountsList,
    std::osyncstream& outputFile
);

void calculateAddressStatisticsOfBlock(
    uint32_t workerIndex,
    const json& block,
    const utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const TxCountsList& txCountsList,
    std::osyncstream& outputFile
);

void calculateAddressStatisticsOfTx(
    uint32_t workerIndex,
    const json& tx,
    const utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const TxCountsList& txCountsList,
    std::osyncstream& outputFile,
    bool isMiningTx
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

    const std::string txCountsFilePath = argumentParser.get("--tx_counts_file");
    logger.info(fmt::format("Load tx counts from {}", txCountsFilePath));
    TxCountsList txCountsList(quickUnion.getSize(), std::make_pair(0, 0));
    logger.info(fmt::format("Loaded tx counts from {}", txCountsFilePath));

    logUsedMemory();

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);
    uint32_t workerIndex = 0;
    std::vector<std::future<void>> tasks;

    std::ofstream outputFile(outputFilePath.c_str());
    std::osyncstream syncOutputFile(outputFile);
    std::string tableTitle = "User,Type,TotalCount,TxValue,BlockIndex,Fee,Weight,IsMining";
    syncOutputFile << tableTitle << std::endl;

    for (const auto& taskChunk : taskChunks) {
        tasks.push_back(
            std::async(
                generateEntityTxsOfDays,
                workerIndex,
                &taskChunk,
                &quickUnion,
                &clusterLabels,
                &txCountsList,
                &syncOutputFile
            )
        );

        ++workerIndex;
    }
    utils::waitForTasks(logger, tasks);

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

    program.add_argument("--entity_label_file")
        .help("Entity label file")
        .required();

    program.add_argument("--tx_counts_file")
        .help("Tx counts file")
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

void generateEntityTxsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    const utils::btc::WeightedQuickUnion* quickUnion,
    const std::vector<utils::btc::ClusterLabels>* clusterLabels,
    const TxCountsList* txCountsList,
    std::osyncstream* outputFile
) {
    logger.info(fmt::format("<{}> Worker started", workerIndex));

    for (const auto& dayDir : *daysDirList) {
        calculateAddressStatisticsOfDays(
            workerIndex,
            dayDir,
            *quickUnion,
            *clusterLabels,
            *txCountsList,
            *outputFile
        );
    }
}


void calculateAddressStatisticsOfDays(
    uint32_t workerIndex,
    const std::string& dayDir,
    const utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const TxCountsList& txCountsList,
    std::osyncstream& outputFile
) {
    try {
        auto convertedBlocksFilePath = fmt::format("{}/{}", dayDir, "converted-block-list.json");
        logger.info(fmt::format("<{}> Process combined blocks file: {}", workerIndex, dayDir));

        std::ifstream convertedBlocksFile(convertedBlocksFilePath.c_str());
        if (!convertedBlocksFile.is_open()) {
            logger.warning(fmt::format("Skip processing blocks by date because file not exists: {}", convertedBlocksFilePath));
            return;
        }

        logUsedMemory();
        json blocks;
        convertedBlocksFile >> blocks;
        logger.info(fmt::format("<{}> Block count: {} {}", workerIndex, dayDir, blocks.size()));
        logUsedMemory();

        for (const auto& block : blocks) {
            calculateAddressStatisticsOfBlock(
                workerIndex,
                block,
                quickUnion,
                clusterLabels,
                txCountsList,
                outputFile
            );
        }

        logUsedMemory();

        logger.info(fmt::format("<{}> Finished process blocks by date: {}", workerIndex, dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("<{}> Error when process blocks by date: {}", workerIndex, dayDir));
        logger.error(e.what());
    }
}

void calculateAddressStatisticsOfBlock(
    uint32_t workerIndex,
    const json& block,
    const utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const TxCountsList& txCountsList,
    std::osyncstream& outputFile
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        const auto& txs = block["tx"];
        uint32_t txIndex = 0;
        for (const auto& tx : txs) {
            calculateAddressStatisticsOfTx(
                workerIndex,
                tx,
                quickUnion,
                clusterLabels,
                txCountsList,
                outputFile,
                txIndex == 0
            );

            ++txIndex;
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("<{}> Error when process block {}", workerIndex, blockHash));
        logger.error(e.what());
    }
}

class TxItem {
public:
    BtcId userId;
    // 0: in, 1: out
    uint8_t type;
    uint64_t txCount;
    uint64_t txValue;
    uint32_t blockIndex;
    uint64_t fee;
    uint64_t weight;
    bool isMining;
    utils::btc::ClusterLabels clusterLabel;

    std::string format() {
        return fmt::format("{},{},{},{},{},{},{},{},{},{},{}",
            userId,
            type,
            txCount,
            txValue,
            blockIndex,
            fee,
            weight,
            isMining,
            clusterLabel.isLabeldExchange,
            clusterLabel.isFoundExchange,
            clusterLabel.isMiner
        );
    }
};

void calculateAddressStatisticsOfTx(
    uint32_t workerIndex,
    const json& tx,
    const utils::btc::WeightedQuickUnion& quickUnion,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    const TxCountsList& txCountsList,
    std::osyncstream& outputFile,
    bool isMiningTx
) {
    logger.info(fmt::format("<{}> [TX] Before tx info", workerIndex));
    std::string txHash = utils::json::get(tx, "hash");
    logger.info(fmt::format("<{}> [TX] Tx hash: {}", workerIndex, txHash));
    uint32_t blockIndex = utils::json::get(tx, "block_index");
    logger.info(fmt::format("<{}> [TX] Block index: {}", workerIndex, blockIndex));
    uint64_t fee = utils::json::get(tx, "fee");
    logger.info(fmt::format("<{}> [TX] Tx fee: {}", workerIndex, fee));
    uint64_t weight = utils::json::get(tx, "weight");
    logger.info(fmt::format("<{}> [TX] Tx weight: {}", workerIndex, weight));
    logger.info(fmt::format("<{}> [TX] After tx info", workerIndex));

    logger.info(fmt::format("<{}> [TX] Before tx process", workerIndex));
    try {
        const auto& inputs = utils::json::get(tx, "inputs");
        uint64_t inputTotalValue = 0;
        BtcId inputEntityId = 0;
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
            inputEntityId = quickUnion.findRoot(addressId);

            const auto valueItem = prevOut.find("value");
            if (valueItem == prevOut.cend()) {
                continue;
            }
            uint64_t inputValue = valueItem.value();
            inputTotalValue += inputValue;
        }

        if (inputTotalValue > 0) {
            TxItem inputTxItem{
                .userId=inputEntityId,
                .type = 0,
                .txCount = txCountsList[inputEntityId].first,
                .txValue = inputTotalValue,
                .blockIndex = utils::json::get(tx, "block_index"),
                .fee = utils::json::get(tx, "fee"),
                .weight = utils::json::get(tx, "weight"),
                .isMining = false,
                .clusterLabel = clusterLabels[inputEntityId]
            };

            std::string outputLine = inputTxItem.format();
            outputFile << outputLine << std::endl;
        }

        auto& outputs = utils::json::get(tx, "out");
        for (auto& output : outputs) {
            auto addrItem = output.find("addr");
            if (addrItem == output.cend()) {
                continue;
            }
            BtcId addressId = addrItem.value();
            BtcId outputEntityId = quickUnion.findRoot(addressId);

            const auto valueItem = output.find("value");
            if (valueItem == output.cend()) {
                continue;
            }
            uint64_t outputValue = valueItem.value();

            TxItem outputTxItem{
                .userId=addressId,
                .type = 1,
                .txCount = txCountsList[outputEntityId].second,
                .txValue = outputValue,
                .blockIndex = utils::json::get(tx, "block_index"),
                .fee = 0,
                .weight = 0,
                .isMining = isMiningTx,
                .clusterLabel = clusterLabels[outputEntityId]
            };

            std::string outputLine = outputTxItem.format();
            outputFile << outputLine << std::endl;
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("<{}> Error when process tx: {}", workerIndex, txHash));
        logger.error(e.what());
    }

    logger.info(fmt::format("<{}> [TX] After tx process", workerIndex));
}

std::size_t loadCountList(
    const std::string& inputFilePath,
    TxCountsList& countList
) {
    logger.info(fmt::format("Load count from: {}", inputFilePath));
    std::ifstream inputFile(inputFilePath.c_str(), std::ios::binary);
    std::size_t loadedCount = 0;

    std::size_t countSize = countList.size();
    inputFile.read(reinterpret_cast<char*>(&loadedCount), sizeof(loadedCount));
    inputFile.read(reinterpret_cast<char*>(countList.data()), countSize * sizeof(countList[0]));

    return loadedCount;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}