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
#include <argparse/argparse.hpp>

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

static argparse::ArgumentParser createArgumentParser();

using json = nlohmann::json;
using WeightedQuickUnionPtr = std::shared_ptr<utils::btc::WeightedQuickUnion>;

inline BtcId parseMaxId(const char* maxIdArg);

std::unique_ptr<std::vector<WeightedQuickUnionPtr>> unionFindByWorkers(
    BtcId maxId,
    const std::vector<std::string>& daysList,
    uint32_t initialWorkerCount,
    const std::string& dayInputsFileName
);

std::vector<WeightedQuickUnionPtr> mergeQuickUnionsByWorkers(
    std::unique_ptr<std::vector<WeightedQuickUnionPtr>>& quickFindUnions,
    uint32_t maxMergeWorkerCount
);

WeightedQuickUnionPtr unionFindTxInputsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    BtcId maxId,
    const std::string& dayInputsFileName
);

void unionFindTxInputsOfDay(
    const std::string& dayDir,
    WeightedQuickUnionPtr quickUnion,
    const std::string& dayInputsFileName
);

WeightedQuickUnionPtr moveMergeQuickUnions(
    std::vector<WeightedQuickUnionPtr>* quickUnions
);

inline void logUsedMemory();

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

    std::string daysListFilePath = argumentParser.get("days_dir_list");
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));
    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    BtcId maxId = argumentParser.get<BtcId>("--id_max_value");
    uint32_t initialWorkerCount = argumentParser.get<uint32_t>("--worker_count");
    std::string dayInputsFileName = argumentParser.get("--day_ins_file");

    logUsedMemory();
    auto quickFindUnions = unionFindByWorkers(maxId, daysList, initialWorkerCount, dayInputsFileName);
    logUsedMemory();

    uint32_t maxMergeWorkerCount = argumentParser.get<uint32_t>("--merge_worker_count");
    auto firstMergedQuickFindUnions = mergeQuickUnionsByWorkers(quickFindUnions, maxMergeWorkerCount);
    logUsedMemory();

    logger.info("Do final merge");

    WeightedQuickUnionPtr mergedQuickFindUnions = moveMergeQuickUnions(&firstMergedQuickFindUnions);
    mergedQuickFindUnions->save(argumentParser.get("result_file"));
    logger.info(fmt::format("Found entities: {}", mergedQuickFindUnions->getClusterCount()));

    logUsedMemory();

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_union_find");

    program.add_argument("days_dir_list")
        .required()
        .help("List file path of days directories");

    program.add_argument("result_file")
        .help("The file path of result file")
        .required();

    program.add_argument("--id_max_value")
        .help("The max value of BTC Id")
        .scan<'d', BtcId>()
        .required();

    program.add_argument("--day_ins_file")
        .help("Filename of day inputs address file")
        .default_value("day-inputs.json");

    program.add_argument("-w", "--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    program.add_argument("--merge_worker_count")
        .help("Max merge worker count")
        .scan<'d', uint32_t>()
        .required();

    return program;
}

std::unique_ptr<std::vector<WeightedQuickUnionPtr>> unionFindByWorkers(
    BtcId maxId,
    const std::vector<std::string>& daysList,
    uint32_t initialWorkerCount,
    const std::string& dayInputsFileName
) {

    uint32_t workerCount = std::min(initialWorkerCount, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);
    uint32_t workerIndex = 0;
    std::vector<std::future<WeightedQuickUnionPtr>> tasks;
    for (const auto& taskChunk : taskChunks) {
        tasks.push_back(
            std::async(unionFindTxInputsOfDays, workerIndex, &taskChunk, maxId, dayInputsFileName)
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
    std::unique_ptr<std::vector<WeightedQuickUnionPtr>>& quickFindUnions,
    uint32_t maxMergeWorkerCount
) {
    uint32_t firstMergeWorkerCount = std::min(maxMergeWorkerCount, std::thread::hardware_concurrency());
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
    BtcId maxId,
    const std::string& dayInputsFileName
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    auto quickUnion = std::make_shared<utils::btc::WeightedQuickUnion>(maxId);

    for (const auto& dayDir : *daysList) {
        unionFindTxInputsOfDay(dayDir, quickUnion, dayInputsFileName);
    }

    return quickUnion;
}

void unionFindTxInputsOfDay(
    const std::string& dayDir,
    WeightedQuickUnionPtr quickUnion,
    const std::string& dayInputsFileName
) {
    try {
        auto txInputsOfDayFilePath = fmt::format("{}/{}", dayDir, dayInputsFileName);
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