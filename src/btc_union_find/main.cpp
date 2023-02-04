#include "btc-config.h"
#include "btc_gen_day_ins/logger.h"

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
#include <memory>

namespace fs = std::filesystem;

using json = nlohmann::json;
using WeightedQuickUnionPtr = std::shared_ptr<utils::btc::WeightedQuickUnion>;

WeightedQuickUnionPtr unionFindTxInputsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    BtcId maxId
);

void generateTxInputsOfDay(
    const std::string& dayDir,
    const std::map<std::string, BtcId>& address2Id
);

inline std::vector<std::vector<BtcId>> generateTxInputsOfBlock(
    const std::string& dayDir,
    const json& block,
    const std::map<std::string, BtcId>& address2Id
);

inline std::vector<BtcId> generateTxInputs(
    const std::string& dayDir,
    const json& tx,
    const std::map<std::string, BtcId>& address2Id
);

inline void dumpDayInputs(
    const char* filePath,
    std::vector<std::vector<std::vector<BtcId>>> txInputsOfDay
);

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_union_find <days_dir_list> <id_max_value>\n" << std::endl;

        return EXIT_FAILURE;
    }

    const char* daysListFilePath = argv[1];
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    uint32_t workerCount = std::min(BTC_UNION_FIND_WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    logUsedMemory();

    BtcId maxId = 0;
    try {
       maxId = std::stoi(argv[2]);

       if (maxId == 0) {
           logger.error("id_max_value must greater than zero!");

           return EXIT_FAILURE;
       }
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Can't parse value {} as id: {}", argv[2], e.what()));

        return EXIT_FAILURE;
    }

    std::vector<std::shared_ptr<utils::btc::WeightedQuickUnion>> unionFindIdsOfTasks;
    //for (int32_t taskIndex = 0; taskIndex != workerCount; ++taskIndex) {
    //    unionFindIdsOfTasks.push_back(std::make_shared<utils::btc::WeightedQuickUnion>(maxId));
    //}

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);
    uint32_t workerIndex = 0;
    std::vector<std::future<WeightedQuickUnionPtr>> tasks;
    for (const auto& taskChunk : taskChunks) {
        tasks.push_back(
            std::async(unionFindTxInputsOfDays, workerIndex, &taskChunk, maxId)
        );

        ++workerIndex;
    }
    utils::waitForTasks(logger, tasks);

    logUsedMemory();

    std::vector<WeightedQuickUnionPtr> quickFindUnions;
    for (auto& task : tasks) {
        quickFindUnions.push_back(WeightedQuickUnionPtr(std::move(task.get())));
    }

    workerIndex = 0;
    for (auto& quickFindUnion : quickFindUnions) {
        logger.info(fmt::format("Worker {} result: {} {}", workerIndex, quickFindUnion.use_count(), quickFindUnion->getCount()));

        ++workerIndex;
    }

    logUsedMemory();

    for (auto& quickFindUnion : quickFindUnions) {
        logger.info(fmt::format("Worker {} result: {} {}", workerIndex, quickFindUnion.use_count(), quickFindUnion->getCount()));
        quickFindUnion.reset();
    }

    logUsedMemory();

    return EXIT_SUCCESS;
}

WeightedQuickUnionPtr unionFindTxInputsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysList,
    BtcId maxId
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    //for (const auto& dayDir : *daysList) {
    //    generateTxInputsOfDay(dayDir, *address2Id);
    //}

    return std::make_shared<utils::btc::WeightedQuickUnion>(maxId);
}

void generateTxInputsOfDay(
    const std::string& dayDir,
    const std::map<std::string, BtcId>& address2Id
) {
    try {
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

        std::vector<std::vector<std::vector<BtcId>>> txInputsOfDay;
        for (const auto& block : blocks) {
            const auto& txInputsOfBlock = generateTxInputsOfBlock(dayDir, block, address2Id);
            if (txInputsOfBlock.size() > 0) {
                txInputsOfDay.push_back(txInputsOfBlock);
            }
        }

        logger.info(fmt::format("Dump tx inputs of {} blocks by date: {}", txInputsOfDay.size(), dayDir));

        logUsedMemory();

        auto txInputsOfDayFilePath = fmt::format("{}/{}", dayDir, "day-inputs.json");
        dumpDayInputs(txInputsOfDayFilePath.c_str(), txInputsOfDay);

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

inline std::vector<std::vector<BtcId>> generateTxInputsOfBlock(
    const std::string& dayDir,
    const json& block,
    const std::map<std::string, BtcId>& address2Id
) {
    std::string blockHash = utils::json::get(block, "hash");
    std::vector<std::vector<BtcId>> inputIdsOfBlock;

    try {
        const auto& txs = block["tx"];

        for (const auto& tx : txs) {
            const auto& inputIds = generateTxInputs(dayDir, tx, address2Id);

            if (inputIds.size() > 0) {
                inputIdsOfBlock.push_back(inputIds);
            }
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}:{}", dayDir, blockHash));
        logger.error(e.what());
    }

    return inputIdsOfBlock;
}

inline std::vector<BtcId> generateTxInputs(
    const std::string& dayDir,
    const json& tx,
    const std::map<std::string, BtcId>& address2Id
) {
    std::string txHash = utils::json::get(tx, "hash");
    std::vector<BtcId> inputIds;

    try {
        const auto& inputs = utils::json::get(tx, "inputs");
        for (const auto& input : inputs) {
            const auto prevOutItem = input.find("prev_out");
            if (prevOutItem == input.cend()) {
                continue;
            }
            const auto& prevOut = prevOutItem.value();

            const auto addrItem = prevOut.find("addr");
            if (addrItem != prevOut.cend()) {
                std::string address = addrItem.value();
                auto inputIdIt = address2Id.find(address);
                if (inputIdIt != address2Id.cend()) {
                    inputIds.push_back(inputIdIt->second);
                }
            }
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}:{}", dayDir, txHash));
        logger.error(e.what());
    }

    if (inputIds.size() > 0) {
        std::sort(inputIds.begin(), inputIds.end());
        auto newEnd = std::unique(inputIds.begin(), inputIds.end());
        if (newEnd != inputIds.end()) {
            inputIds.erase(newEnd, inputIds.end());
        }
    }

    return inputIds;
}

void dumpDayInputs(
    const char* filePath,
    std::vector<std::vector<std::vector<BtcId>>> txInputsOfDay
) {
    logger.info(fmt::format("Dump day_ins: {}", filePath));

    json txInputsOfDayJson(txInputsOfDay);
    std::ofstream txInputsOfDayFile(filePath);
    txInputsOfDayFile << txInputsOfDayJson;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}