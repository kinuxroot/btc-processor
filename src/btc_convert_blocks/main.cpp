#include "btc-config.h"
#include "btc_convert_blocks/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/btc_utils.h"
#include "utils/mem_utils.h"
#include "fmt/format.h"
#include <nlohmann/json.hpp>

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

using json = nlohmann::json;

namespace fs = std::filesystem;

void convertBlocksOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    const std::map<std::string, BtcId>* address2Id,
    bool skipExisted
);

void convertBlocksOfDay(
    const std::string& dayDir,
    const std::map<std::string, BtcId>& address2Id,
    bool skipExisted
);

inline std::vector<std::vector<BtcId>> convertAddressesOfBlock(
    const std::string& dayDir,
    json& block,
    const std::map<std::string, BtcId>& address2Id
);

inline void convertAddressesOfTx(
    const std::string& dayDir,
    json& tx,
    const std::map<std::string, BtcId>& address2Id
);

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_convert_blocks <days_dir_list> <id2addr> <skip_existed>\n" << std::endl;

        return EXIT_FAILURE;
    }

    const char* daysListFilePath = argv[1];
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    uint32_t workerCount = std::min(BTC_CONVERT_BLOCKS_WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    logUsedMemory();

    const char* id2AddressFilePath = argv[2];
    logger.info("Load address2Id...");
    const auto& address2Id = utils::btc::loadAddress2Id(id2AddressFilePath);
    logger.info(fmt::format("Loaded address2Id: {} items", address2Id.size()));

    logUsedMemory();

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);

    logUsedMemory();

    bool skipExisted = false;
    if (argc == 4) {
        std::string skipExistedStr = argv[3];
        skipExisted = skipExistedStr == "true";
    }

    uint32_t workerIndex = 0;
    std::vector<std::future<void>> tasks;
    for (const auto& taskChunk : taskChunks) {
        tasks.push_back(
            std::async(convertBlocksOfDays, workerIndex, &taskChunk, &address2Id, skipExisted)
        );

        ++workerIndex;
    }
    utils::waitForTasks(logger, tasks);

    return EXIT_SUCCESS;
}

void convertBlocksOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysList,
    const std::map<std::string, BtcId>* address2Id,
    bool skipExisted
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    for (const auto& dayDir : *daysList) {
        convertBlocksOfDay(dayDir, *address2Id, skipExisted);
    }
}

void convertBlocksOfDay(
    const std::string& dayDir,
    const std::map<std::string, BtcId>& address2Id,
    bool skipExisted
) {
    try {
        auto convertedBlocksListFilePath = fmt::format("{}/{}", dayDir, "converted-block-list.json");
        if (skipExisted && fs::exists(convertedBlocksListFilePath)) {
            logger.info(fmt::format("Skip existed blocks by date: {}", dayDir));

            return;
        }

        auto combinedBlocksFilePath = fmt::format("{}/{}", dayDir, "combined-block-list.json");
        logger.info(fmt::format("Process combined blocks file: {}", dayDir));

        std::ifstream combinedBlocksFile(combinedBlocksFilePath.c_str());
        if (!combinedBlocksFile.is_open()) {
            logger.warning(fmt::format("Finished process blocks by date because file not exists: {}", combinedBlocksFilePath));
            return;
        }

        logUsedMemory();
        json blocks;
        combinedBlocksFile >> blocks;
        logger.info(fmt::format("Block count: {} {}", dayDir, blocks.size()));
        logUsedMemory();

        for (auto& block : blocks) {
            convertAddressesOfBlock(dayDir, block, address2Id);
        }

        logUsedMemory();
        std::ofstream convertedBlocksFile(convertedBlocksListFilePath.c_str());
        convertedBlocksFile << blocks;

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

inline std::vector<std::vector<BtcId>> convertAddressesOfBlock(
    const std::string& dayDir,
    json& block,
    const std::map<std::string, BtcId>& address2Id
) {
    std::string blockHash = utils::json::get(block, "hash");
    std::vector<std::vector<BtcId>> inputIdsOfBlock;

    try {
        auto& txs = block["tx"];

        for (auto& tx : txs) {
            convertAddressesOfTx(dayDir, tx, address2Id);
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}:{}", dayDir, blockHash));
        logger.error(e.what());
    }

    return inputIdsOfBlock;
}

inline void convertAddressesOfTx(
    const std::string& dayDir,
    json& tx,
    const std::map<std::string, BtcId>& address2Id
) {
    std::string txHash = utils::json::get(tx, "hash");

    try {
        auto& inputs = utils::json::get(tx, "inputs");
        for (auto& input : inputs) {
            auto prevOutItem = input.find("prev_out");
            if (prevOutItem == input.cend()) {
                continue;
            }
            auto& prevOut = prevOutItem.value();

            auto addrItem = prevOut.find("addr");
            if (addrItem != prevOut.cend()) {
                std::string address = addrItem.value();
                auto addressIdItem = address2Id.find(address);

                if (addressIdItem != address2Id.end()) {
                    BtcId addressId = addressIdItem->second;
                    addrItem.value() = addressId;
                }
            }
        }

        auto& outputs = utils::json::get(tx, "out");
        for (auto& output : outputs) {
            auto addrItem = output.find("addr");
            if (addrItem != output.cend()) {
                std::string address = addrItem.value();
                auto addressIdItem = address2Id.find(address);

                if (addressIdItem != address2Id.end()) {
                    BtcId addressId = addressIdItem->second;
                    addrItem.value() = addressId;
                }
            }
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}:{}", dayDir, txHash));
        logger.error(e.what());
    }
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}