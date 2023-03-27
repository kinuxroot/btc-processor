#include "btc-config.h"
#include "btc_match_exchange_address/logger.h"
#include <argparse/argparse.hpp>

#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "fmt/format.h"

#include <cstdlib>
#include <iostream>

using utils::btc::BtcSize;

inline void logUsedMemory();

struct ExchangeWalletEntry {
    std::string name;
    std::string sampleAddress;
    BtcSize addressCount;
};

struct ExchangeWalletMatchResult {
    std::string name;
    std::string sampleAddress;
    BtcSize addressCount;
    BtcId clusterId;
    BtcSize clusterSize;
};

static argparse::ArgumentParser createArgumentParser();
static std::vector<ExchangeWalletEntry> readExchangeWalletEntries(const std::string& filePath);
static std::vector<ExchangeWalletMatchResult> matchExchangeWalletEntries(
    uint32_t workerIndex,
    const std::vector<ExchangeWalletEntry>* entries,
    const std::map<std::string, BtcId>* addr2Ids,
    const utils::btc::WeightedQuickUnion* quickUnion
);

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

    try {
        std::string exchangeWalletFilePath = argumentParser.get("exchange_wallet_file");
        const auto& exchangeWalletEntries = readExchangeWalletEntries(exchangeWalletFilePath);
        logUsedMemory();

        for (const auto& exchangeWalletEntry : exchangeWalletEntries) {
            std::cout << fmt::format("{},{},{}\n",
                exchangeWalletEntry.name,
                exchangeWalletEntry.addressCount,
                exchangeWalletEntry.sampleAddress
            );
        }

        uint32_t workerCount = std::min(
            argumentParser.get<uint32_t>("--worker_count"),
            std::thread::hardware_concurrency()
        );
        logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
        logger.info(fmt::format("Worker count: {}", workerCount));

        const auto& taskChunks = utils::generateTaskChunks<ExchangeWalletEntry>(exchangeWalletEntries, workerCount);

        std::string id2AddressFilePath = argumentParser.get("id2addr");
        logger.info(fmt::format("Load address2Id from {}...", id2AddressFilePath));
        const auto& addr2Ids = utils::btc::loadAddress2Id(id2AddressFilePath.c_str());
        logUsedMemory();

        std::string unionFindFilePath = argumentParser.get("uf_file");
        utils::btc::WeightedQuickUnion quickUnion(1);
        quickUnion.load(unionFindFilePath);

        logger.info(fmt::format("Loaded ids: {}", quickUnion.getSize()));
        logger.info(fmt::format("Loaded cluster count: {}", quickUnion.getClusterCount()));
        logUsedMemory();

        std::string outputFilePath = argumentParser.get("output_file");
        std::ofstream outputFile(outputFilePath.c_str());

        uint32_t workerIndex = 0;
        std::vector<std::future<std::vector<ExchangeWalletMatchResult>>> tasks;
        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(
                std::async(matchExchangeWalletEntries, workerIndex, &taskChunk, &addr2Ids, &quickUnion)
            );

            ++workerIndex;
        }
        logUsedMemory();

        logger.info(fmt::format("Dump result to: {}", outputFilePath));

        std::vector<ExchangeWalletMatchResult> exchangeWalletResults;
        for (auto& task : tasks) {
            std::vector<ExchangeWalletMatchResult> currentResults = task.get();
            for (const auto& currentResult : currentResults) {
                outputFile << fmt::format("{} {} {} {} {}\n",
                    currentResult.name,
                    currentResult.clusterSize,
                    currentResult.addressCount,
                    currentResult.sampleAddress,
                    currentResult.clusterId
                );
            }
        }

        logUsedMemory();

        logUsedMemory();
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_match_exchange_address");

    program.add_argument("id2addr")
        .help("Id2address file")
        .required();

    program.add_argument("uf_file")
        .help("Union find output file")
        .required();

    program.add_argument("exchange_wallet_file")
        .help("Exchange wallet file")
        .required();

    program.add_argument("output_file")
        .help("Output result file")
        .required();

    program.add_argument("-w", "--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    return program;
}

std::vector<ExchangeWalletEntry> readExchangeWalletEntries(const std::string& filePath) {
    std::ifstream inputFile(filePath);
    std::vector<ExchangeWalletEntry> entries;

    while (inputFile) {
        ExchangeWalletEntry entry;
        inputFile >> entry.name;

        if (entry.name.empty()) {
            break;
        }

        inputFile >> entry.addressCount >> entry.sampleAddress;
        entries.push_back(entry);
    }

    return entries;
}

static std::vector<ExchangeWalletMatchResult> matchExchangeWalletEntries(
    uint32_t workerIndex,
    const std::vector<ExchangeWalletEntry>* entries,
    const std::map<std::string, BtcId>* addr2Ids,
    const utils::btc::WeightedQuickUnion* quickUnion
) {
    std::vector<ExchangeWalletMatchResult> matchResults;
    for (const auto& entry : *entries) {
        auto addressIdIt = addr2Ids->find(entry.sampleAddress);
        if (addressIdIt == addr2Ids->end()) {
            logger.error(fmt::format("Can't find address: {}", entry.sampleAddress));

            continue;
        }

        BtcId addressId = addressIdIt->second;
        BtcId addressClusterId = quickUnion->findRoot(addressId);
        BtcSize addressClusterSize = quickUnion->getClusterSize(addressClusterId);

        if (addressClusterSize == 0) {
            logger.error(fmt::format("Can't find cluster: {},{}", entry.sampleAddress, addressId));

            continue;
        }

        matchResults.push_back(ExchangeWalletMatchResult {
            .name = entry.name,
            .sampleAddress = entry.sampleAddress,
            .addressCount = entry.addressCount,
            .clusterId = addressClusterId,
            .clusterSize = addressClusterSize,
        });
    }

    return matchResults;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}