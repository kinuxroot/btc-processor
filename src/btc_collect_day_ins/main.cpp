#include "btc-config.h"
#include "btc_gen_address/logger.h"

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
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace fs = std::filesystem;

void getInputBtcIdOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    std::set<BtcId>* addresses
);

void getInputBtcIdOfDay(
    const std::string& dayDir,
    std::set<BtcId>& addresses
);

void mergeInputBtcIds(
    std::vector<std::set<BtcId>>& tasksUniqueAddresses
);

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_collect_day_ins <days_dir_list>\n" << std::endl;

        return EXIT_FAILURE;
    }

    const char* daysListFilePath = argv[1];
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    uint32_t workerCount = std::min(BTC_COLLECT_DAY_INS_WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);
    std::vector<std::set<BtcId>> tasksUniqueAddresses(workerCount);

    uint32_t workerIndex = 0;
    std::vector<std::future<void>> tasks;
    for (const auto& taskChunk : taskChunks) {
        auto& taskUniqueeAddresses = tasksUniqueAddresses[workerIndex];
        tasks.push_back(
            std::async(getInputBtcIdOfDays, workerIndex, &taskChunk, &taskUniqueeAddresses)
        );

        ++workerIndex;
    }
    utils::waitForTasks(logger, tasks);

    mergeInputBtcIds(tasksUniqueAddresses);

    return EXIT_SUCCESS;
}

void getInputBtcIdOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysList,
    std::set<BtcId>* addresses
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    for (const auto& dayDir : *daysList) {
        getInputBtcIdOfDay(dayDir, *addresses);
        auto usedMemory = utils::mem::getAllocatedMemory();
        logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
    }
}

void getInputBtcIdOfDay(
    const std::string& dayDir,
    std::set<BtcId>& addresses
) {
    try {
        auto txInputsOfDayFilePath = fmt::format("{}/{}", dayDir, "day-inputs.json");
        std::vector<std::vector<std::vector<BtcId>>> txInputsOfDay;
        utils::btc::loadDayInputs(txInputsOfDayFilePath.c_str(), txInputsOfDay);

        for (const auto& txs : txInputsOfDay) {
            for (const auto& inputs : txs) {
                if (inputs.size() == 0) {
                    continue;
                }

                auto firstId = inputs[0];
                for (const auto input : inputs) {
                    addresses.insert(input);
                }
            }
        }

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

void mergeInputBtcIds(
    std::vector<std::set<BtcId>>& tasksUniqueAddresses
) {
    std::set<BtcId> finalAddressSet;
    size_t totalAddressCount = 0;
    while (tasksUniqueAddresses.size()) {
        const auto& taskUniqueAddresses = tasksUniqueAddresses.back();
        logger.info(fmt::format("Generated task unique addresses: {}", taskUniqueAddresses.size()));
        totalAddressCount += taskUniqueAddresses.size();

        finalAddressSet.insert(taskUniqueAddresses.cbegin(), taskUniqueAddresses.cend());
        tasksUniqueAddresses.pop_back();
    }

    logger.info(fmt::format("Final unique addresses: {}/{}", finalAddressSet.size(), totalAddressCount));
    logger.info(fmt::format("Remove duplicated addresses: {}", totalAddressCount - finalAddressSet.size()));
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}