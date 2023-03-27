#include "btc-config.h"
#include "btc_match_exchange_address/logger.h"
#include <argparse/argparse.hpp>

#include "utils/io_utils.h"
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

static argparse::ArgumentParser createArgumentParser();
static std::vector<ExchangeWalletEntry> readExchangeWalletEntries(const std::string& filePath);

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
        //std::string id2AddressFilePath = argumentParser.get("id2addr");
        //logger.info(fmt::format("Load address2Id from {}...", id2AddressFilePath));
        //const auto& id2Address = utils::btc::loadId2Address(id2AddressFilePath.c_str());

        //std::string unionFindFilePath = argumentParser.get("uf_file");
        //utils::btc::WeightedQuickUnion quickUnion(1);
        //quickUnion.load(unionFindFilePath);

        //logger.info(fmt::format("Loaded ids: {}", quickUnion.getSize()));
        //logger.info(fmt::format("Loaded cluster count: {}", quickUnion.getClusterCount()));

        std::string exchangeWalletFilePath = argumentParser.get("exchange_wallet_file");
        const auto& exchangeWalletEntries = readExchangeWalletEntries(exchangeWalletFilePath);

        for (const auto& exchangeWalletEntry : exchangeWalletEntries) {
            std::cout << fmt::format("{},{},{}",
                exchangeWalletEntry.name,
                exchangeWalletEntry.addressCount,
                exchangeWalletEntry.sampleAddress
            );
        }


        //const char* outputFilePath = argv[2];
        //std::ofstream outputFile(outputFilePath);
        //logger.info(fmt::format("Dump union find result to: {}", outputFilePath));

        //std::set<BtcId> exchangeRootAddresseIds;
        //if (argc == 4) {
        //    const char* exclusiveFilePath = argv[3];
        //    std::vector<BtcId> addressIds = utils::readLines<std::vector<BtcId>, BtcId>(
        //        exclusiveFilePath,
        //        [](const std::string& line) -> BtcId {
        //            return std::stoi(line);
        //        }
        //    );

        //    for (BtcId addressId : addressIds) {
        //        exchangeRootAddresseIds.insert(quickUnion.findRoot(addressId));
        //    }
        //}
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

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}