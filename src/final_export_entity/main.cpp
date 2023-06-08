// 导出用户表

#include "btc-config.h"
#include "final_export_entity/logger.h"

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
        // 解析矿工地址ID
        std::string minerTxJsonFilePath = argumentParser.get("--miner_json");
        logger.info("Load miner json...");
        const auto& minerAddressIds = parseMinerAddressIds(minerTxJsonFilePath);
        logger.info(fmt::format("Found miner address ids: {}", minerAddressIds.size()));

        logUsedMemory();

        // 加载UF File
        std::string quickUnionFilePath = argumentParser.get("--uf_file");
        utils::btc::WeightedQuickUnion quickUnion(1);
        std::set<BtcId> minterTxRootIds;
        logUsedMemory();
        logger.info(fmt::format("Load quick union file: {}", quickUnionFilePath));
        quickUnion.load(quickUnionFilePath);
        logger.info(fmt::format("Loaded quick union file: {}", quickUnionFilePath));

        // 计算矿工用户集合
        logger.info(fmt::format("Computing miner users"));
        for (BtcId minerAddressId : minerAddressIds) {
            minterTxRootIds.insert(quickUnion.findRoot(minerAddressId));
        }
        logger.info(fmt::format("Finished computing miner users: {}", minterTxRootIds.size()));

        logUsedMemory();

        // 加载交易所实体（手动收集标签+WalletExplorer）
        std::string exchangeEntityListFilePath = argumentParser.get("--exchange_entity_list_file");
        logger.info(fmt::format("Load exchange entity list file: {}", exchangeEntityListFilePath));
        const auto& exchangeEntities = utils::readLines<std::set<BtcId>, BtcId>(
            exchangeEntityListFilePath,
            [](const std::string& line) -> BtcId {
                return std::stoi(line);
            },
            [](std::set<BtcId>& results, BtcId value) {
                results.insert(value);
            }
        );
        logger.info(fmt::format("Loaded exchange entity list: {}", exchangeEntities.size()));

        // 加载交易所地址（矿工交易+交易频率最高）
        std::string minerTxCombinedListFilePath = argumentParser.get("--miner_tx_combined_list");
        logger.info(fmt::format("Load miner tx combined list file: {}", minerTxCombinedListFilePath));
        const auto& minerTxCombinedAddresses = utils::readLines<std::vector<BtcId>, BtcId>(
            minerTxCombinedListFilePath,
            [](const std::string& line) -> BtcId {
                return std::stoi(line);
            }
        );
        logger.info(fmt::format("Loaded miner tx combined list file: {}", minerTxCombinedAddresses.size()));

        // 计算交易所实体（矿工交易+交易频率最高）
        std::set<BtcId> minerTxCombinedAddressesRootIds;
        logger.info(fmt::format("Computing miner tx combined users"));
        for (BtcId addressId : minerTxCombinedAddresses) {
            minerTxCombinedAddressesRootIds.insert(quickUnion.findRoot(addressId));
        }
        logger.info(fmt::format("Finished miner tx combined users: {}", minerTxCombinedAddressesRootIds.size()));

        // 导出用户列表报告
        std::string outputReportFilePath = argumentParser.get("output_report_file");
        std::ofstream outputReportFile(outputReportFilePath);
        utils::btc::WeightedQuickUnionClusters quickUnionClusters(quickUnion);

        logger.info(fmt::format("Export report file: {}", outputReportFilePath));
        outputReportFile << fmt::format("User,IsMiner,IsLabeldExchange,IsFoundExchange") << std::endl;
        std::vector<utils::btc::ClusterLabels> clusterLabels(quickUnion.getSize());
        quickUnionClusters.forEach(
            [&outputReportFile, &minterTxRootIds, &exchangeEntities, &minerTxCombinedAddressesRootIds, &clusterLabels]
            (BtcId entityId, BtcId btcSize) -> void {
                bool isMinerUser = minterTxRootIds.contains(entityId);
                bool isMuanualExchange = exchangeEntities.contains(entityId);
                bool isMinerTxCombinedAddressRootId = minerTxCombinedAddressesRootIds.contains(entityId);

                outputReportFile << fmt::format("{},{},{},{}",
                    entityId, isMinerUser, isMuanualExchange, isMinerTxCombinedAddressRootId
                ) << std::endl;

                clusterLabels[entityId].isMiner = isMinerUser;
                clusterLabels[entityId].isLabeldExchange = isMuanualExchange;
                clusterLabels[entityId].isFoundExchange = isMinerTxCombinedAddressRootId;
            }
        );

        logUsedMemory();

        // 导出用户标签记录
        std::string outputLabelFilePath = argumentParser.get("output_label_file");
        logger.info(fmt::format("Export label file: {}", outputLabelFilePath));

        std::ofstream outputLabelFile(outputLabelFilePath, std::ios::binary);
        outputLabelFile.write(
            reinterpret_cast<char*>(clusterLabels.data()),
            clusterLabels.size() * sizeof(utils::btc::ClusterLabels)
        );
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_filter_address_balance");

    program.add_argument("output_report_file")
        .required()
        .help("The output report file path");

    program.add_argument("output_label_file")
        .required()
        .help("The output label file path");

    program.add_argument("--uf_file")
        .help("The Union find file path")
        .required();

    program.add_argument("--miner_json")
        .help("The JSON file of miner txs")
        .default_value("");

    program.add_argument("--exchange_entity_list_file")
        .help("The exchange entity list file")
        .required();

    program.add_argument("--miner_tx_combined_list")
        .help("The combined list file of miner txs and top txs")
        .required();

    return program;
}

std::set<BtcId> parseMinerAddressIds(
    const std::string& minerTxJsonFilePath
) {
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