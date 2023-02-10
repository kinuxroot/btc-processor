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

inline BtcId parseMaxId(const char* maxIdArg);

std::unique_ptr<std::vector<WeightedQuickUnionPtr>> unionFindByWorkers(
    BtcId maxId,
    const std::vector<std::string>& daysList
);

std::vector<WeightedQuickUnionPtr> mergeQuickUnionsByWorkers(
    std::unique_ptr<std::vector<WeightedQuickUnionPtr>>& quickFindUnions
);

WeightedQuickUnionPtr unionFindTxInputsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    BtcId maxId
);

void unionFindTxInputsOfDay(
    const std::string& dayDir,
    WeightedQuickUnionPtr quickUnion
);

WeightedQuickUnionPtr moveMergeQuickUnions(
    std::vector<WeightedQuickUnionPtr>* quickUnions
);

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_union_find <days_dir_list> <id_max_value> <result_file>\n" << std::endl;

        return EXIT_FAILURE;
    }

    const char* daysListFilePath = argv[1];
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    BtcId maxId = parseMaxId(argv[2]);
    if (!maxId) {
        return EXIT_FAILURE;
    }
    logUsedMemory();
    
    auto quickFindUnions = unionFindByWorkers(maxId, daysList);
    logUsedMemory();

    auto firstMergedQuickFindUnions = mergeQuickUnionsByWorkers(quickFindUnions);
    logUsedMemory();

    logger.info("Do final merge");

    WeightedQuickUnionPtr mergedQuickFindUnions = moveMergeQuickUnions(&firstMergedQuickFindUnions);
    mergedQuickFindUnions->save(argv[3]);
    logger.info(fmt::format("Found entities: {}", mergedQuickFindUnions->getClusterCount()));

    logUsedMemory();

    return EXIT_SUCCESS;
}

inline BtcId parseMaxId(const char* maxIdArg) {
    BtcId maxId = 0;
    try {
        maxId = std::stoi(maxIdArg);

        if (maxId == 0) {
            logger.error("id_max_value must greater than zero!");

            return maxId;
        }
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Can't parse value {} as id: {}", maxIdArg, e.what()));

        return maxId;
    }

    return maxId;
}

std::unique_ptr<std::vector<WeightedQuickUnionPtr>> unionFindByWorkers(
    BtcId maxId,
    const std::vector<std::string>& daysList
) {

    uint32_t workerCount = std::min(BTC_UNION_FIND_WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

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

    auto quickFindUnions = std::make_unique<std::vector<WeightedQuickUnionPtr>>();
    for (auto& task : tasks) {
        quickFindUnions->push_back(WeightedQuickUnionPtr(std::move(task.get())));
    }

    return quickFindUnions;
}

std::vector<WeightedQuickUnionPtr> mergeQuickUnionsByWorkers(
    std::unique_ptr<std::vector<WeightedQuickUnionPtr>>& quickFindUnions
) {
    uint32_t firstMergeWorkerCount = std::min(BTC_UNION_FIND_INNER_MERGE_WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("First merge worker count: {}", firstMergeWorkerCount));
    auto firstMergeQuickFindUnionChunks = utils::generateTaskChunks(*quickFindUnions, firstMergeWorkerCount);

    logUsedMemory();

    quickFindUnions.release();

    logUsedMemory();

    logger.info("Generating first merge tasks");

    uint32_t firstMergeWorkerIndex = 0;
    std::vector<std::future<WeightedQuickUnionPtr>> firstMergeTasks;
    for (auto& taskChunk : firstMergeQuickFindUnionChunks) {
        firstMergeTasks.push_back(
            std::async(moveMergeQuickUnions, &taskChunk)
        );

        ++firstMergeWorkerIndex;
    }
    utils::waitForTasks(logger, firstMergeTasks);

    logUsedMemory();

    std::vector<WeightedQuickUnionPtr> firstMergedQuickFindUnions;
    for (auto& task : firstMergeTasks) {
        firstMergedQuickFindUnions.push_back(WeightedQuickUnionPtr(std::move(task.get())));
    }

    return firstMergedQuickFindUnions;
}

WeightedQuickUnionPtr unionFindTxInputsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysList,
    BtcId maxId
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    auto quickUnion = std::make_shared<utils::btc::WeightedQuickUnion>(maxId);

    for (const auto& dayDir : *daysList) {
        unionFindTxInputsOfDay(dayDir, quickUnion);
    }

    return quickUnion;
}

void unionFindTxInputsOfDay(
    const std::string& dayDir,
    WeightedQuickUnionPtr quickUnion
) {
    try {
        auto txInputsOfDayFilePath = fmt::format("{}/{}", dayDir, "day-inputs.json");
        std::vector<std::vector<std::vector<BtcId>>> txInputsOfDay;
        utils::btc::loadDayInputs(txInputsOfDayFilePath.c_str(), txInputsOfDay);

        for (const auto& txs : txInputsOfDay) {
            for (const auto& inputs : txs) {
                if (inputs.size() <= 1) {
                    continue;
                }

                auto firstId = inputs[0];
                for (const auto input : inputs) {
                    quickUnion->connect(firstId, input);
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

WeightedQuickUnionPtr moveMergeQuickUnions(
    std::vector<WeightedQuickUnionPtr>* quickUnions
) {
    if (quickUnions->size() == 0) {
        return WeightedQuickUnionPtr();
    }

    WeightedQuickUnionPtr finalQuickUnion = (*quickUnions)[0];
    for (auto quickUnionIt = quickUnions->begin() + 1; quickUnionIt != quickUnions->end(); ++quickUnionIt) {
        auto& quickUnion = *quickUnionIt;

        finalQuickUnion->merge(*quickUnion);

        // Reset pointer to release memory
        quickUnion.reset();
    }

    return finalQuickUnion;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}