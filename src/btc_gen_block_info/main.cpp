#include "btc_gen_block_info/logger.h"

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
    std::string nextBlockOutputDirPath
);

std::tuple<std::vector<std::string>, std::vector<std::string>> generateBlocksInfoOfDay(
    const std::string& dayDir,
    std::ofstream& completedBlockOutputFile,
    std::ofstream& nextBlockOutputFile
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

    uint32_t workerCount = std::min(
        argumentParser.get<uint32_t>("--worker_count"),
        std::thread::hardware_concurrency()
    );
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    logUsedMemory();

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);

    std::string completedBlockOutputDirPath = argumentParser.get("complete_block_output_dir");
    fs::create_directories(completedBlockOutputDirPath);
    std::string nextBlockOutputDirPath = argumentParser.get("next_block_output_dir");
    fs::create_directories(nextBlockOutputDirPath);

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
                nextBlockOutputDirPath
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

    program.add_argument("next_block_output_dir")
        .required()
        .help("Output directory path of next blocks");

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
    std::string nextBlockOutputDirPath
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    std::string completedBlockOutputFilePath = fmt::format("{}/{:0>2}.list", completedBlockOutputDirPath, workerIndex);
    std::cout << completedBlockOutputFilePath << std::endl;
    std::ofstream completedBlockOutputFile(completedBlockOutputFilePath);

    std::string nextBlockOutputFilePath = fmt::format("{}/{:0>2}.list", nextBlockOutputDirPath, workerIndex);
    std::cout << nextBlockOutputFilePath << std::endl;
    std::ofstream nextBlockOutputFile(nextBlockOutputFilePath);

    for (const auto& dayDir : *daysDirList) {
        const auto& dayBlocksInfo = generateBlocksInfoOfDay(dayDir, completedBlockOutputFile, nextBlockOutputFile);

        const auto& completedBlockHashes = std::get<0>(dayBlocksInfo);
        std::string completedBlockHashesString;
        for (const auto& blockHash : completedBlockHashes) {
            completedBlockHashesString.append(blockHash).append("\n");
        }
        completedBlockOutputFile << completedBlockHashesString;
        logger.info(fmt::format("Output completed block addresses: {}", completedBlockHashes.size()));

        const auto& nextBlockOutputHashes = std::get<1>(dayBlocksInfo);
        std::string nextBlockOutputHashesString;
        for (const auto& blockHash : nextBlockOutputHashes) {
            nextBlockOutputHashesString.append(blockHash).append("\n");
        }
        nextBlockOutputFile << nextBlockOutputHashesString;
        logger.info(fmt::format("Output next block addresses: {}", nextBlockOutputHashes.size()));
    }
}

std::tuple<std::vector<std::string>, std::vector<std::string>> generateBlocksInfoOfDay(
    const std::string& dayDir,
    std::ofstream& completedBlockOutputFile,
    std::ofstream& nextBlockOutputFile
) {
    std::vector<std::string> completedBlockHashes;
    std::vector<std::string> nextBlockOutputHashes;

    try {
        auto convertedBlocksFilePath = fmt::format("{}/{}", dayDir, "converted-block-list.json");
        logger.info(fmt::format("Process combined blocks file: {}", dayDir));

        std::ifstream convertedBlocksFile(convertedBlocksFilePath.c_str());
        if (!convertedBlocksFile.is_open()) {
            logger.warning(fmt::format("Finished process blocks by date because file not exists: {}", convertedBlocksFilePath));
            return std::make_tuple(completedBlockHashes, nextBlockOutputHashes);
        }

        logUsedMemory();
        json blocks;
        convertedBlocksFile >> blocks;
        logger.info(fmt::format("Block count: {} {}", dayDir, blocks.size()));
        logUsedMemory();

        completedBlockHashes.reserve(blocks.size());
        nextBlockOutputHashes.reserve(blocks.size());
        for (const auto& block : blocks) {
            std::string blockHash = utils::json::get(block, "hash");
            completedBlockHashes.push_back(blockHash);

            json nextHashes = utils::json::get(block, "next_block");
            for (const json& nextHash : nextHashes) {
                nextBlockOutputHashes.push_back(nextHash);
            }
        }

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }

    return std::make_tuple(completedBlockHashes, nextBlockOutputHashes);
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}
