#include "btc-config.h"
#include "final_export_union_find/logger.h"

#include "utils/io_utils.h"
#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "fmt/format.h"
#include <argparse/argparse.hpp>

#include <cstdlib>
#include <iostream>

inline void logUsedMemory();

auto& logger = getLogger();

static argparse::ArgumentParser createArgumentParser();

int main(int argc, char* argv[]) {
    using utils::btc::BtcSize;

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
        std::string id2AddressFilePath = argumentParser.get("id2addr");
        logger.info("Load address2Id...");
        const auto& address2Id = utils::btc::loadId2Address(id2AddressFilePath.c_str());
        logger.info(fmt::format("Loaded address2Id: {} items", address2Id.size()));

        logUsedMemory();

        std::string quickUnionFilePath = argumentParser.get("--uf_file");
        logger.info(fmt::format("Load union find form {}", quickUnionFilePath));

        utils::btc::WeightedQuickUnion quickUnion(1);
        quickUnion.load(quickUnionFilePath);

        logger.info(fmt::format("Loaded ids: {}", quickUnion.getSize()));
        logger.info(fmt::format("Loaded cluster count: {}", quickUnion.getClusterCount()));

        logUsedMemory();

        std::string outputFilePath = argumentParser.get("output_file");
        std::ofstream outputFile(outputFilePath.c_str());
        logger.info(fmt::format("Dump union find result to: {}", outputFilePath));

        BtcId addressId = 0;
        std::string lines;
        for (const std::string address : address2Id) {
            auto rootAddressId = quickUnion.findRoot(addressId);
            std::string line = fmt::format("{},{}\n", rootAddressId, address);
            lines.append(line);

            if (addressId && addressId % 1000000 == 0) {
                outputFile << lines;
                lines.clear();
            }

            ++addressId;
        }

        if (!lines.empty()) {
            outputFile << lines;
        }
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_export_union_find");

    program.add_argument("id2addr")
        .required()
        .help("The file path of id2addr");

    program.add_argument("output_file")
        .required()
        .help("The output file path");

    program.add_argument("--uf_file")
        .help("The Union find file path")
        .default_value("");

    return program;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}