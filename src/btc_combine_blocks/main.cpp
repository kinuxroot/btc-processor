#include "btc-config.h"
#include "btc_combine_blocks/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "fmt/format.h"

#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <thread>
#include <future>
#include <algorithm>

namespace fs = std::filesystem;

void combineBlocksOfDays(uint32_t workerIndex, const std::vector<std::string>& daysList);
void combineBlocksFromList(const fs::path& dayDirPath);

auto& logger = getLogger();

int main(int32_t argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_combine_blocks <days_lists>\n" << std::endl;

        return EXIT_FAILURE;
    }
    
    try {
        std::vector<std::string> daysList;
        int32_t inputFileCount = argc - 1;
        logger.info(fmt::format("List file count: {}", inputFileCount));
        for (int32_t inputFileIndex = 0; inputFileIndex != inputFileCount; ++inputFileIndex) {
            const char* daysListFilePath = argv[inputFileIndex + 1];
            logger.info(fmt::format("Read tasks form {}", daysListFilePath));

            utils::readLines(daysListFilePath, daysList);
        }
        logger.info(fmt::format("Read tasks count: {}", daysList.size()));

        uint32_t workerCount = std::min(BTC_COMBINE_BLOCKS_WORKER_COUNT, std::thread::hardware_concurrency());
        logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
        logger.info(fmt::format("Worker count: {}", workerCount));

        const auto& taskChunks = utils::generateTaskChunks(daysList, workerCount);
        for (const auto& taskChunk : taskChunks) {
            logger.info(fmt::format("Task chunk size: {}", taskChunk.size()));
        }

        uint32_t workerIndex = 0;
        std::vector<std::future<void>> tasks;
        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(std::async(combineBlocksOfDays, workerIndex, taskChunk));

            ++workerIndex;
        }
        utils::waitForTasks(logger, tasks);
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

void combineBlocksOfDays(uint32_t workerIndex, const std::vector<std::string>& daysList) {
    logger.info(fmt::format("Combine task started: {}", workerIndex));

    for (const auto& dayDirPath : daysList) {
        combineBlocksFromList(fs::path(dayDirPath));
    }
}

void combineBlocksFromList(const fs::path& dayDirPath) {
    logger.info(fmt::format("Combine blocks by date: {}", dayDirPath.string()));
    
    std::vector<fs::path> blockFilePaths;

    try {
        auto combinedBlocksFilePath = dayDirPath / "combined-block-list.json";
        if (fs::exists(combinedBlocksFilePath)) {
            logger.info(fmt::format("Skip combining blocks by date: {}", dayDirPath.string()));

            return;
        }

        std::ofstream combinedBlockFile(combinedBlocksFilePath);
        combinedBlockFile << "[";

        bool isFirstBlock = true;
        for (auto const& dayDirEntry : std::filesystem::directory_iterator{ dayDirPath })
        {
            if (!dayDirEntry.is_regular_file()) {
                continue;
            }

            const auto& blockFilePath = dayDirEntry.path();

            auto entryFileName = dayDirEntry.path().filename().string();
            if (entryFileName == "combined-block-list.json") {
                continue;
            }

            auto entryExt = dayDirEntry.path().extension().string();
            if (entryExt != ".json") {
                continue;
            }

            if (!isFirstBlock) {
                combinedBlockFile << ",";
            }
            else {
                isFirstBlock = false;
            }

            std::ifstream blockFile(blockFilePath.c_str(), std::ios::binary);
            utils::copyStream(blockFile, combinedBlockFile);
            
            blockFilePaths.push_back(dayDirEntry.path());
        }

        combinedBlockFile << "]";

        logger.info(fmt::format("Finished combining blocks by date: {}", dayDirPath.string()));
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when combining blocks by date: {}", dayDirPath.string()));
        logger.error(e.what());

        return;
    }

    try {
        logger.info(fmt::format("Removed combined block files by date: {}", dayDirPath.string()));

        for (const auto& blockFilePath : blockFilePaths) {
            fs::remove(blockFilePath);
        }

        logger.info(fmt::format("Finished Removing combined block files by date: {}", dayDirPath.string()));
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when removed combined block files by date: {}", dayDirPath.string()));
        logger.error(e.what());
    }
}