#include "btc-config.h"
#include "btc_gen_day_ins/logger.h"

#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "fmt/format.h"
#include <argparse/argparse.hpp>

#include <cstdlib>
#include <iostream>
#include <thread>
#include <filesystem>
#include <iomanip>

namespace fs = std::filesystem;
using BalanceValue = double;
using BalanceList = std::vector<BalanceValue>;

inline void logUsedMemory();

auto& logger = getLogger();

static argparse::ArgumentParser createArgumentParser();
std::vector<std::string> getAddressBalancePaths(
    const std::string& dirPath,
    uint32_t startYear,
    uint32_t endYear
);
std::set<BtcId> loadExcludeRootAddresses(
    const std::string& excludeAddressListFilePath,
    const utils::btc::WeightedQuickUnion& quickUnion
);
std::size_t loadBalanceList(
    const std::string& inputFilePath,
    BalanceList& balanceList
);
void processAddressBalanceOfYears(
    uint32_t workerIndex,
    const std::vector<std::string>* addressBalanceFilePaths,
    const std::string& outputBaseDir,
    utils::btc::WeightedQuickUnion* quickUnion,
    const std::set<BtcId>* excludeAddresses
);
void processYearAddressBalance(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceFilePath,
    utils::btc::WeightedQuickUnion& quickUnion,
    const std::set<BtcId>& excludeAddresses
);
void checkYearAddressBalance(
    const std::string& addressBalanceFilePath
);

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
        std::string balanceBaseDirPath = argumentParser.get("balance_base_dir");

        auto startYear = argumentParser.get<uint32_t>("--start_year");
        logger.info(fmt::format("Using start year: {}", startYear));

        auto endYear = argumentParser.get<uint32_t>("--end_year");
        logger.info(fmt::format("Using end year: {}", endYear));

        uint32_t workerCount = std::min(argumentParser.get<uint32_t>("--worker_count"), std::thread::hardware_concurrency());
        logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
        logger.info(fmt::format("Worker count: {}", workerCount));

        const std::vector<std::string>& addressBalanceFiles = getAddressBalancePaths(
            balanceBaseDirPath, startYear, endYear
        );
        const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(addressBalanceFiles, workerCount);

        const std::string ufFilePath = argumentParser.get("--union_file");
        utils::btc::WeightedQuickUnion quickUnion(1);
        logger.info(fmt::format("Load quickUnion from {}", ufFilePath));
        quickUnion.load(ufFilePath);
        logger.info(fmt::format("Loaded quickUnion {} items from {}", quickUnion.getSize(), ufFilePath));

        const std::string excludeAddressListFilePath = argumentParser.get("--exclude_addrs");
        const std::set<BtcId>& excludeRootAddresses = loadExcludeRootAddresses(excludeAddressListFilePath, quickUnion);

        std::string outputBaseDirPath = argumentParser.get("output_base_dir");

        uint32_t workerIndex = 0;
        std::vector<std::future<void>> tasks;
        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(
                std::async(
                    processAddressBalanceOfYears,
                    workerIndex,
                    &taskChunk,
                    outputBaseDirPath,
                    &quickUnion,
                    &excludeRootAddresses
                )
            );

            ++workerIndex;
        }
        logUsedMemory();

        utils::waitForTasks(logger, tasks);
        logUsedMemory();
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_filter_address_balance");

    program.add_argument("balance_base_dir")
        .required()
        .help("The base directory of address balance list files");

    program.add_argument("output_base_dir")
        .required()
        .help("The base directory of entity balance list files");

    program.add_argument("--union_file")
        .help("Union find file")
        .required();

    program.add_argument("-e", "--exclude_addrs")
        .help("Exclude addresses file path")
        .default_value("");

    program.add_argument("--start_year")
        .help("Start year")
        .scan<'d', uint32_t>()
        .default_value(0u);

    program.add_argument("--end_year")
        .help("Start year")
        .scan<'d', uint32_t>()
        .default_value(0u);

    program.add_argument("-w", "--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    return program;
}

std::vector<std::string> getAddressBalancePaths(
    const std::string& dirPath,
    uint32_t startYear,
    uint32_t endYear
) {
    std::vector<std::string> addressBalanceFilePaths;

    for (auto const& dirEntry : std::filesystem::directory_iterator{ dirPath })
    {
        if (dirEntry.is_directory()) {
            continue;
        }

        const auto& filePath = dirEntry.path();
        if (!filePath.has_extension()) {
            continue;
        }

        const auto fileExtension = filePath.extension().string();
        if (fileExtension != ".out") {
            continue;
        }

        const auto baseFilename = filePath.stem();
        uint32_t currentYear = std::stoi(baseFilename);
        if ((startYear && currentYear < startYear) || (endYear && currentYear > endYear)) {
            continue;
        }

        addressBalanceFilePaths.push_back(filePath.string());
        logger.info(fmt::format("Add balance file to task: {}", filePath.string()));
    }

    return addressBalanceFilePaths;
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

void processAddressBalanceOfYears(
    uint32_t workerIndex,
    const std::vector<std::string>* addressBalanceFilePaths,
    const std::string& outputBaseDir,
    utils::btc::WeightedQuickUnion* quickUnion,
    const std::set<BtcId>* excludeAddresses
) {
    fs::path outputBaseDirPath(outputBaseDir);

    for (const std::string& addressBalanceFilePath : *addressBalanceFilePaths) {
        auto entityBalanceFilePath = outputBaseDirPath / fs::path(addressBalanceFilePath).filename();

        //if (fs::exists(entityBalanceFilePath)) {
        //    logger.info(fmt::format("Skip existed entityBalanceFilePath: {}", entityBalanceFilePath.string()));

        //    return;
        //}

        //processYearAddressBalance(
        //    addressBalanceFilePath,
        //    entityBalanceFilePath.string(),
        //    *quickUnion,
        //    *excludeAddresses
        //);

        checkYearAddressBalance(addressBalanceFilePath);
    }
}

void checkYearAddressBalance(
    const std::string& addressBalanceFilePath
) {
    using utils::btc::BtcSize;

    BalanceList balanceList;
    loadBalanceList(addressBalanceFilePath, balanceList);

    for (BalanceValue balance : balanceList) {
        if (balance < 0) {
            logger.error(fmt::format("Error balance: {}", balance));
        }
    }
}

void processYearAddressBalance(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceFilePath,
    utils::btc::WeightedQuickUnion& quickUnion,
    const std::set<BtcId>& exchangeRootAddresseIds
) {
    using utils::btc::BtcSize;

    BalanceList balanceList;
    loadBalanceList(addressBalanceFilePath, balanceList);

    std::vector<BalanceValue> clusterBalances(balanceList.size(), 0.0);
    BtcId currentAddressId = 0;
    logger.info("Calculate balance list of entities");
    for (auto balance : balanceList) {
        BtcId entityId = quickUnion.findRoot(currentAddressId);

        if (entityId >= balanceList.size()) {
            logger.error(fmt::format("Entity id {} is greater than balance array size: {}", entityId, balanceList.size()));
            continue;
        }
        clusterBalances.at(entityId) += balance;

        ++currentAddressId;
    }

    const auto& quickUnionClusters = utils::btc::WeightedQuickUnionClusters(quickUnion);
    BtcSize dumpedClusterCount = 0;
    BtcSize skippedClusterCount = 0;
    BtcSize skippedAddressCount = 0;
    std::ofstream outputFile(entityBalanceFilePath.c_str());
    logger.info(fmt::format("Output balance list of entities to {}", entityBalanceFilePath));
    quickUnionClusters.forEach([
        &outputFile, &exchangeRootAddresseIds, &clusterBalances,
            &skippedClusterCount, &dumpedClusterCount, &skippedAddressCount
    ](BtcId btcId, BtcSize btcSize) {
        if (exchangeRootAddresseIds.contains(btcId)) {
            skippedAddressCount += btcSize;
            ++skippedClusterCount;

            return;
        }

        if (btcId >= clusterBalances.size()) {
            logger.error(fmt::format("Entity id {} is greater than balance array size: {}", btcId, clusterBalances.size()));

            return;
        }

        auto balance = clusterBalances[btcId];
        outputFile << btcId << "," << btcSize << "," << std::setprecision(19) << balance << std::endl;

        ++dumpedClusterCount;

        if (dumpedClusterCount % 100000 == 0) {
            logger.info(fmt::format("Dump clusters: {}", dumpedClusterCount));
        }
    });

    logger.info(fmt::format("dumpedClusterCount: {}", dumpedClusterCount));
    logger.info(fmt::format("skippedClusterCount: {}", skippedClusterCount));
    logger.info(fmt::format("skippedAddressCount: {}", skippedAddressCount));
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

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}