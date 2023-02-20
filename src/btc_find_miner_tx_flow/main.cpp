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

struct MinerTxFlow {
    uint64_t prev_out_tx_index;
    BtcId prev_out_tx_addr;
    int64_t input_value;
    BtcId output_addr;
    int64_t output_value;
    uint64_t output_tx_index;
    std::string output_script;
    std::size_t output_n;
};

void generateMinerTxFlowsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    const MinerTxs* minerTxs
);

void generateMinerTxFlowsOfDay(
    const std::string& dayDir,
    const MinerTxs& minerTxs
);

void generateMinerTxFlowsOfBlock(
    const std::string& dayDir,
    const json& block,
    std::vector<MinerTxFlow>& minerTxFlows,
    const MinerTxs& minerTxs,
    const std::string& filePath,
    std::size_t blockOffset
);

MinerTxsPtr loadMinerTxs(const char* filePath);

void generateMinerTxFlowsOfTx(
    const std::string& dayDir,
    const json& tx,
    std::vector<MinerTxFlow>& minerTxFlows,
    const MinerTxs& minerTxs
);

void dumpMinerTxFlows(
    const std::string& filePath,
    const std::vector<MinerTxFlow>& minerTxFlows
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

    uint32_t workerCount = std::min(BTC_FIND_MINER_TX_FLOW_WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    const char* mergedTxsFilePath = argv[2];
    MinerTxsPtr minerTxs = loadMinerTxs(mergedTxsFilePath);

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);
    std::vector<std::set<BtcId>> tasksUniqueAddresses(workerCount);

    uint32_t workerIndex = 0;
    std::vector<std::future<void>> tasks;
    for (const auto& taskChunk : taskChunks) {
        auto& taskUniqueeAddresses = tasksUniqueAddresses[workerIndex];
        tasks.push_back(
            std::async(generateMinerTxFlowsOfDays, workerIndex, &taskChunk, minerTxs.get())
        );

        ++workerIndex;
    }

    utils::waitForTasks(logger, tasks);

    return EXIT_SUCCESS;
}

void generateMinerTxFlowsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    const MinerTxs* minerTxs
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    for (const auto& dayDir : *daysDirList) {
        generateMinerTxFlowsOfDay(dayDir, *minerTxs);
        logUsedMemory();
    }
}

void generateMinerTxFlowsOfDay(
    const std::string& dayDir,
    const MinerTxs& minerTxs
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

        std::vector<MinerTxFlow> minerTxFlows;
        std::size_t blockOffset = 0;
        for (const auto& block : blocks) {
            generateMinerTxFlowsOfBlock(dayDir, block, minerTxFlows, minerTxs, convertedBlocksFilePath, blockOffset);
            ++blockOffset;
        }
        logger.info(fmt::format("Finished {} minerTxFlows of date: {}", minerTxFlows.size(), dayDir));
        auto minerTxFlowsFilePath = fmt::format("{}/{}", dayDir, "miner-tx-flows.csv");
        dumpMinerTxFlows(minerTxFlowsFilePath, minerTxFlows);

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

void generateMinerTxFlowsOfBlock(
    const std::string& dayDir,
    const json& block,
    std::vector<MinerTxFlow>& minerTxFlows,
    const MinerTxs& minerTxs,
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

        for (const auto& tx : txs) {
            generateMinerTxFlowsOfTx(dayDir, tx, minerTxFlows, minerTxs);
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}:{}", dayDir, blockHash));
        logger.error(e.what());
    }
}

void generateMinerTxFlowsOfTx(
    const std::string& dayDir,
    const json& tx,
    std::vector<MinerTxFlow>& minerTxFlows,
    const MinerTxs& minerTxs
) {
    std::string txHash = utils::json::get(tx, "hash");

    try {
        const auto& inputs = utils::json::get(tx, "inputs");
        if (!inputs.is_array()) {
            throw std::invalid_argument("inouts must be an array");
        }

        if (!inputs.size()) {
            throw std::invalid_argument("inouts must have elements");
        }

        std::vector<json> minerTxInputs;
        for (const auto& input : inputs) {
            const auto prevOutItem = input.find("prev_out");
            if (prevOutItem == input.cend()) {
                continue;
            }
            const auto& prevOut = prevOutItem.value();
            if (prevOut.is_null()) {
                continue;
            }

            const auto addrItem = prevOut.find("addr");
            if (addrItem == prevOut.cend()) {
                continue;
            }

            const auto txIndexItem = prevOut.find("tx_index");
            if (txIndexItem == prevOut.cend()) {
                continue;
            }

            uint64_t txIndex = txIndexItem.value();
            if (txIndex == 0) {
                continue;
            }

            const auto txIndexMinerTxIt = minerTxs.find(txIndex);
            if (txIndexMinerTxIt == minerTxs.cend()) {
                continue;
            }

            const auto& txIndexMinerTx = txIndexMinerTxIt->second;
            const auto& txIndexMinerOutputs = txIndexMinerTx["outputs"];
            std::size_t outputOffset = prevOut["n"];
            if (outputOffset >= txIndexMinerOutputs.size()) {
                throw std::out_of_range(fmt::format(
                    "outputOffset {} is greater than txIndexMinerOutputs.size() {}", 
                    outputOffset, txIndexMinerOutputs.size()
                ));
            }

            BtcId prevOutAddr = addrItem.value();
            json minerTxItem = txIndexMinerOutputs[outputOffset];
            BtcId minerTxAddr = minerTxItem["addr"];
            if (prevOutAddr != minerTxAddr) {
                logger.debug(prevOut.dump(1));
                logger.debug(minerTxItem.dump(1));
                throw std::invalid_argument(fmt::format("prevOutAddr({}) != minerTxAddr({})", prevOutAddr, minerTxAddr));
            }

            json minerTxInput = {
                {"prev_out_tx_index", txIndex},
                {"prev_out_tx_addr", addrItem.value()},
                {"input_value", prevOut["value"]},
            };

            minerTxInputs.push_back(minerTxInput);
        }

        if (!minerTxInputs.size()) {
            return;
        }

        const auto& outputs = utils::json::get(tx, "out");
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

            for (const auto& minerTxInput : minerTxInputs) {
                minerTxFlows.push_back({
                    .prev_out_tx_index = minerTxInput["prev_out_tx_index"],
                    .prev_out_tx_addr = minerTxInput["prev_out_tx_addr"],
                    .input_value = minerTxInput["input_value"],
                    .output_addr = output["addr"],
                    .output_value = output["value"],
                    .output_tx_index = output["tx_index"],
                    .output_script = output["script"],
                    .output_n = output["n"],
                });
            }
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}:{}", dayDir, txHash));
        logger.error(e.what());

        std::rethrow_exception(std::current_exception());
    }
}

MinerTxsPtr loadMinerTxs(const char* filePath) {
    logger.info(fmt::format("Load minerTxsList: {}", filePath));

    std::ifstream minerTxsListFile(filePath);
    json minerTxsListJson;
    minerTxsListFile >> minerTxsListJson;

    MinerTxsPtr minerTxs = std::make_unique<MinerTxs>();
    if (!minerTxsListJson.is_array()) {
        std::cerr << fmt::format("txs must be array: {}", filePath) << std::endl;

        return minerTxs;
    }

    if (!minerTxsListJson.size()) {
        return minerTxs;
    }

    for (const auto& minerTxJson : minerTxsListJson) {
        if (!minerTxJson.is_array()) {
            std::cerr << fmt::format("miner tx json must be array: {}", filePath) << std::endl;

            return minerTxs;
        }

        if (minerTxJson.size() != 2) {
            std::cerr << fmt::format("size of miner tx json must be 2: {}", filePath) << std::endl;

            return minerTxs;
        }

        uint64_t txId = minerTxJson[0];
        json tx = minerTxJson[1];

        (*minerTxs)[txId] = tx;
    }

    logger.info(fmt::format("Loaded minerTxsList: {}", minerTxs->size()));

    return minerTxs;
}

void dumpMinerTxFlows(
    const std::string& filePath,
    const std::vector<MinerTxFlow>& minerTxFlows
) {
    std::ofstream minerTxFlowsFile(filePath.c_str());

    logger.info(fmt::format("Dump miner tx flows: {}", filePath));

    for (const auto& minerTxFlow : minerTxFlows) {
        std::string line = fmt::format("{},{},{},{},{},{},{},{}",
            minerTxFlow.prev_out_tx_index,
            minerTxFlow.prev_out_tx_addr,
            minerTxFlow.input_value,
            minerTxFlow.output_addr,
            minerTxFlow.output_value,
            minerTxFlow.output_tx_index,
            minerTxFlow.output_script,
            minerTxFlow.output_n
        );

        minerTxFlowsFile << line << std::endl;
    }
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}