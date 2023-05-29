// 计算实体余额

#include "btc-config.h"
#include "final_export_address/logger.h"

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

using json = nlohmann::json;

namespace fs = std::filesystem;

inline void logUsedMemory();

auto& logger = getLogger();

static argparse::ArgumentParser createArgumentParser();
std::set<BtcId> parseMinerAddressIds(const std::string& minerTxJsonFilePath);

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
        std::string minerTxJsonFilePath = argumentParser.get("--miner_json");
        logger.info("Load miner json...");
        const auto& minerAddressIds = parseMinerAddressIds(minerTxJsonFilePath);
        logger.info(fmt::format("Found miner address ids: {}", minerAddressIds.size()));

        logUsedMemory();

        std::string id2AddressFilePath = argumentParser.get("id2addr");
        logger.info("Load address2Id...");
        const auto& address2Id = utils::btc::loadId2Address(id2AddressFilePath.c_str());
        logger.info(fmt::format("Loaded address2Id: {} items", address2Id.size()));

        logUsedMemory();

        std::string outputFilePath = argumentParser.get("output_file");
        std::ofstream outputFile(outputFilePath);
        outputFile << "Address,IsMiner" << std::endl;

        BtcId addressId = 0;
        for (const std::string& address : address2Id) {
            bool isMinerAddress = minerAddressIds.find(addressId) != minerAddressIds.end();
            outputFile << fmt::format("{},{}", address, isMinerAddress) << std::endl;

            addressId++;
        }

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

    program.add_argument("id2addr")
        .required()
        .help("The file path of id2addr");

    program.add_argument("output_file")
        .required()
        .help("The output file path");

    program.add_argument("--miner_json")
        .help("The JSON file of miner txs")
        .default_value("");

    return program;
}

std::set<BtcId> parseMinerAddressIds(const std::string& minerTxJsonFilePath) {
    std::set<BtcId> minerAddressIds;

    if (minerTxJsonFilePath.empty()) {
        return minerAddressIds;
    }

    std::ifstream minerTxJsonFile(minerTxJsonFilePath.c_str());
    if (!minerTxJsonFile.is_open()) {
        logger.warning(fmt::format("Skip parse miner address ids: {}", minerTxJsonFilePath));
        return minerAddressIds;
    }

    logger.info(fmt::format("Begin to load miner tx json file {}", minerTxJsonFilePath));
    json minerTxsListJson;
    minerTxJsonFile >> minerTxsListJson;

    logger.info(fmt::format("Loaded miner tx json count: {}", minerTxsListJson.size()));
    logUsedMemory();

    for (const auto& minerTxJson : minerTxsListJson) {
        if (!minerTxJson.is_array()) {
            std::cerr << fmt::format("miner tx json must be array: {}", minerTxJsonFilePath) << std::endl;

            return minerAddressIds;
        }

        if (minerTxJson.size() != 2) {
            std::cerr << fmt::format("size of miner tx json must be 2: {}", minerTxJsonFilePath) << std::endl;

            return minerAddressIds;
        }

        uint64_t txId = minerTxJson[0];
        json tx = minerTxJson[1];

        const auto& outputs = utils::json::get(tx, "outputs");
        if (!outputs.is_array()) {
            throw std::invalid_argument("outputs must be an array");
        }

        if (!outputs.size()) {
            throw std::invalid_argument("outputs must have elements");
        }

        for (const auto& output : outputs) {
            if (output.find("addr") == output.end() || !output["spent"]) {
                continue;
            }

            BtcId outputAddressId = output["addr"];
            minerAddressIds.insert(outputAddressId);
        }
    }

    return minerAddressIds;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}