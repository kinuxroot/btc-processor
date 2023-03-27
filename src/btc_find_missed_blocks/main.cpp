#include "btc_find_missed_blocks/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/btc_utils.h"
#include "utils/mem_utils.h"
#include "fmt/format.h"
#include <nlohmann/json.hpp>
#include <argparse/argparse.hpp>

#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <future>
#include <set>
#include <filesystem>
#include <thread>
#include <iostream>
#include <tuple>

using json = nlohmann::json;

namespace fs = std::filesystem;

static argparse::ArgumentParser createArgumentParser();

void generateBlocksInfo(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    std::string completedBlockOutputDirPath,
    std::string noNextBlockOutputDirPath
);

std::tuple<std::vector<uint32_t>, std::vector<uint32_t>> generateBlocksInfoOfDay(
    const std::string& dayDir,
    std::ofstream& completedBlockOutputFile
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

    uint32_t workerCount = std::min(argumentParser.get<uint32_t>("--worker_count"), std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    logUsedMemory();

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);

    std::string completedBlockOutputDirPath = argumentParser.get("complete_block_output_dir");
    fs::create_directories(completedBlockOutputDirPath);

    std::string noNextBlockOutputDirPath = argumentParser.get("no_next_block_output_dir");
    fs::create_directories(noNextBlockOutputDirPath);

    logUsedMemory();

    uint32_t workerIndex = 0;
    std::vector<std::future<void>> tasks;
    for (const auto& taskChunk : taskChunks) {
        tasks.push_back(
            std::async(
                generateBlocksInfo,
                workerIndex,
                &taskChunk,
                completedBlockOutputDirPath,
                noNextBlockOutputDirPath
            )
        );

        ++workerIndex;
    }
    utils::waitForTasks(logger, tasks);

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_gen_block_info");

    program.add_argument("days_dir_list")
        .required()
        .help("List file path of days directories");

    program.add_argument("complete_block_output_dir")
        .required()
        .help("Output directory path of completed blocks");

    program.add_argument("no_ext_block_output_dir")
        .required()
        .help("Output directory path of no next blocks");

    program.add_argument("--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    return program;
}

void generateBlocksInfo(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    std::string completedBlockOutputDirPath,
    std::string noNextBlockOutputDirPath
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    std::string completedBlockOutputFilePath = fmt::format("{}/{:0>2}.list", completedBlockOutputDirPath, workerIndex);
    std::cout << completedBlockOutputFilePath << std::endl;
    std::ofstream completedBlockOutputFile(completedBlockOutputFilePath);

    std::string noNextBlockOutputFilePath = fmt::format("{}/{:0>2}.list", noNextBlockOutputDirPath, workerIndex);
    std::cout << noNextBlockOutputFilePath << std::endl;
    std::ofstream noNextBlockOutputFile(noNextBlockOutputFilePath);

    for (const auto& dayDir : *daysDirList) {
        const auto& dayBlocksInfo = generateBlocksInfoOfDay(dayDir, completedBlockOutputFile);

        const auto& completedBlockIndexes = std::get<0>(dayBlocksInfo);
        std::string completedBlockIndexesString;
        for (auto blockIndex : completedBlockIndexes) {
            completedBlockIndexesString.append(std::to_string(blockIndex)).append("\n");
        }
        completedBlockOutputFile << completedBlockIndexesString;
        logger.info(fmt::format("Output completed block addresses: {}", completedBlockIndexes.size()));

        const auto& noNextBlockIndexes = std::get<1>(dayBlocksInfo);
        std::string noNextBlockIndexesString;
        for (auto blockIndex : noNextBlockIndexes) {
            noNextBlockIndexesString.append(std::to_string(blockIndex)).append("\n");
        }
        noNextBlockOutputFile << noNextBlockIndexesString;
        logger.info(fmt::format("Output no next block addresses: {}", noNextBlockIndexes.size()));
    }
}

std::tuple<std::vector<uint32_t>, std::vector<uint32_t>> generateBlocksInfoOfDay(
    const std::string& dayDir,
    std::ofstream& completedBlockOutputFile
) {
    std::vector<uint32_t> completedBlockIndexes;
    std::vector<uint32_t> noNextBlockIndexes;

    try {
        auto convertedBlocksFilePath = fmt::format("{}/{}", dayDir, "converted-block-list.json");
        logger.info(fmt::format("Process combined blocks file: {}", dayDir));

        std::ifstream convertedBlocksFile(convertedBlocksFilePath.c_str());
        if (!convertedBlocksFile.is_open()) {
            logger.warning(fmt::format("Finished process blocks by date because file not exists: {}", convertedBlocksFilePath));
            return std::make_tuple(completedBlockIndexes, noNextBlockIndexes);
        }

        logUsedMemory();
        json blocks;
        convertedBlocksFile >> blocks;
        logger.info(fmt::format("Block count: {} {}", dayDir, blocks.size()));
        logUsedMemory();

        completedBlockIndexes.reserve(blocks.size());
        for (const auto& block : blocks) {
            uint32_t blockIndex = utils::json::get(block, "block_index");
            completedBlockIndexes.push_back(blockIndex);

            json nextHashes = utils::json::get(block, "next_block");
            if (nextHashes.size() == 0) {
                noNextBlockIndexes.push_back(blockIndex);
            }
        }

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }

    return std::make_tuple(completedBlockIndexes, noNextBlockIndexes);
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}
