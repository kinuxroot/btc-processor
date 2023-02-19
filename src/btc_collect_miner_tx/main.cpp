#include "btc-config.h"
#include "btc_collect_miner_tx/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/mem_utils.h"
#include "utils/btc_utils.h"
#include "fmt/format.h"

#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <future>
#include <set>
#include <filesystem>
#include <thread>
#include <stdexcept>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
using MinerTxs = std::map<uint64_t, nlohmann::json>;
using MinerTxsPtr = std::unique_ptr<MinerTxs>;

namespace fs = std::filesystem;

MinerTxsPtr getMinerTxsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList
);

void getMinerTxsOfDay(
    const std::string& dayDir,
    MinerTxs& minerTxs
);

void generateMinerTxOfBlock(
    const std::string& dayDir,
    const json& block,
    MinerTxs& minerTxs,
    const std::string& filePath,
    std::size_t blockOffset
);

MinerTxsPtr mergeTxsList(
    std::vector<MinerTxsPtr>& minerTxsList
);

void dumpMinerTxs(
    const char* filePath,
    const MinerTxsPtr& minerTxsList
);

json toMinerTx(
    const std::string& dayDir,
    const json& tx
);

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_collect_miner_tx <days_dir_list> <miner_txs>\n" << std::endl;

        return EXIT_FAILURE;
    }

    const char* daysListFilePath = argv[1];
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    uint32_t workerCount = std::min(BTC_COLLECT_MINER_TX_WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);
    std::vector<std::set<BtcId>> tasksUniqueAddresses(workerCount);

    uint32_t workerIndex = 0;
    std::vector<std::future<MinerTxsPtr>> tasks;
    for (const auto& taskChunk : taskChunks) {
        auto& taskUniqueeAddresses = tasksUniqueAddresses[workerIndex];
        tasks.push_back(
            std::async(getMinerTxsOfDays, workerIndex, &taskChunk)
        );

        ++workerIndex;
    }

    std::vector<MinerTxsPtr> minerTxsList;
    for (auto& task : tasks) {
        minerTxsList.push_back(task.get());
    }

    auto mergedTxs = mergeTxsList(minerTxsList);
    const char* mergedTxsFilePath = argv[2];
    dumpMinerTxs(mergedTxsFilePath, mergedTxs);

    return EXIT_SUCCESS;
}

MinerTxsPtr getMinerTxsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    auto minerTxs = std::make_unique<MinerTxs>();
    for (const auto& dayDir : *daysDirList) {
        getMinerTxsOfDay(dayDir, *minerTxs);
        logUsedMemory();
    }

    return minerTxs;
}

void getMinerTxsOfDay(
    const std::string& dayDir,
    MinerTxs& minerTxs
) {
    try {
        auto convertedBlocksFilePath = fmt::format("{}/{}", dayDir, "converted-block-list.json");
        logger.info(fmt::format("Process combined blocks file: {}", dayDir));

        std::ifstream convertedBlocksFile(convertedBlocksFilePath.c_str());
        if (!convertedBlocksFile.is_open()) {
            logger.warning(fmt::format("Finished process blocks by date because file not exists: {}", convertedBlocksFilePath));
            return;
        }

        logUsedMemory();
        json blocks;
        convertedBlocksFile >> blocks;
        logger.info(fmt::format("Block count: {} {}", dayDir, blocks.size()));
        logUsedMemory();

        std::size_t blockOffset = 0;
        for (const auto& block : blocks) {
            generateMinerTxOfBlock(dayDir, block, minerTxs, convertedBlocksFilePath, blockOffset);
            ++blockOffset;
        }

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

void generateMinerTxOfBlock(
    const std::string& dayDir,
    const json& block,
    MinerTxs& minerTxs,
    const std::string& filePath,
    std::size_t blockOffset
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        const auto& txs = block["tx"];
        if (!txs.is_array()) {
            throw std::invalid_argument("tx must be an array");
        }

        if (!txs.size()) {
            throw std::invalid_argument("tx must have elements");
        }

        const auto& firstTx = txs.at(0);
        auto minerTx = toMinerTx(dayDir, firstTx);
        minerTx["day"] = filePath;
        minerTx["block"] = {
            {"hash", block["hash"]},
            {"block_index", block["block_index"]},
            {"offset", blockOffset},
        };

        minerTxs[minerTx["tx_index"]] = minerTx;
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}:{}", dayDir, blockHash));
        logger.error(e.what());
    }
}

json toMinerTx(
    const std::string& dayDir,
    const json& tx
) {
    std::string txHash = utils::json::get(tx, "hash");

    try {
        const auto& outputs = utils::json::get(tx, "out");
        if (!outputs.is_array()) {
            throw std::invalid_argument("outputs must be an array");
        }

        if (!outputs.size()) {
            throw std::invalid_argument("outputs must have elements");
        }

        return outputs[0];
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}:{}", dayDir, txHash));
        logger.error(e.what());

        std::rethrow_exception(std::current_exception());
    }
}

MinerTxsPtr mergeTxsList(
    std::vector<MinerTxsPtr>& minerTxsList
) {
    MinerTxsPtr mergedMinerTxs;

    while (minerTxsList.size()) {
        auto& currentMinerTxs = minerTxsList.back();
        if (!mergedMinerTxs) {
            mergedMinerTxs = std::move(currentMinerTxs);
        }
        else {
            for (auto& minerTx : *currentMinerTxs) {
                mergedMinerTxs->insert(minerTx);
            }
        }

        minerTxsList.pop_back();
    }

    return mergedMinerTxs;
}

void dumpMinerTxs(
    const char* filePath,
    const MinerTxsPtr& minerTxs
) {
    logger.info(fmt::format("Dump minerTxsList: {}", filePath));

    json minerTxsListJson(*minerTxs);
    std::ofstream minerTxsListFile(filePath);
    minerTxsListFile << minerTxsListJson;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}