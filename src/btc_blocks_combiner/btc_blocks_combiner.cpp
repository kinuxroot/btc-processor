#include "btc-config.h"
#include "btc_blocks_combiner/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
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
uint32_t WORKER_COUNT = 16;

std::vector<std::string> readDaysList(const char* daysListFilePath);
std::vector<std::vector<std::string>> generateTaskChunks(const std::vector<std::string>& daysDirList, uint32_t workerCount);
void combineBlocksOfDays(uint32_t workerIndex, const std::vector<std::string>& daysList);
void combineBlocksFromList(const std::string& listFilePath);

auto& logger = getLogger();

template <class T>
void waitForTasks(std::vector<std::future<T>>& tasks) {
    auto& logger = getLogger();

    int32_t taskIndex = 0;
    for (auto& task : tasks) {
        logger.info(fmt::format("Wait for task stop: {}", taskIndex));
        task.wait();

        ++taskIndex;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_combine_blocks <days_list>\n" << std::endl;

        return EXIT_FAILURE;
    }
    
    try {
        const char* daysListFilePath = argv[1];
        logger.info(fmt::format("Read tasks form {}", daysListFilePath));

        const std::vector<std::string>& daysList = readDaysList(argv[1]);
        logger.info(fmt::format("Read tasks count: {}", daysList.size()));

        uint32_t workerCount = std::min(WORKER_COUNT, std::thread::hardware_concurrency());
        logger.info(fmt::format("Worker count: {}", workerCount));

        const auto& taskChunks = generateTaskChunks(daysList, workerCount);
        for (const auto& taskChunk : taskChunks) {
            logger.info(fmt::format("Task chunk size: {}", taskChunk.size()));
        }

        uint32_t workerIndex = 0;
        std::vector<std::future<void>> tasks;
        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(std::async(combineBlocksOfDays, workerIndex, taskChunk));

            ++workerIndex;
        }
        waitForTasks(tasks);
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

std::vector<std::string> readDaysList(const char* daysListFilePath) {
    std::vector<std::string> daysListIndex;
    std::ifstream daysListIndexFile(daysListFilePath);
    
    if (!daysListIndexFile.is_open()) {
        logger.error(fmt::format("Can't open file {}", daysListFilePath));

        return daysListIndex;
    }

    while (daysListIndexFile) {
        std::string line;
        std::getline(daysListIndexFile, line);

        if (line.size() > 0) {
            daysListIndex.push_back(line);
        }
    }

    return daysListIndex;
}

std::vector<std::vector<std::string>> generateTaskChunks(const std::vector<std::string>& daysDirList, uint32_t workerCount) {
    std::vector<std::vector<std::string>> taskChunks(workerCount, std::vector<std::string>());

    size_t daysTotalCount = daysDirList.size();
    for (size_t dayIndex = 0; dayIndex != daysTotalCount; ++dayIndex) {
        auto taskIndex = dayIndex % workerCount;
        auto& taskChunk = taskChunks[taskIndex];

        taskChunk.push_back(daysDirList[dayIndex]);
    }

    return taskChunks;
}

void combineBlocksOfDays(uint32_t workerIndex, const std::vector<std::string>& daysList) {
    logger.info(fmt::format("Combine task started: {}", workerIndex));

    for (const auto& dayListPath : daysList) {
        combineBlocksFromList(dayListPath);
    }
}

void combineBlocksFromList(const std::string& listFilePath) {
    logger.info(fmt::format("Combine blocks by date: {}", listFilePath));
    
    try {
        std::ifstream blockListFile(listFilePath.c_str());

        std::string blocksDirPath;
        std::getline(blockListFile, blocksDirPath);
        std::string combinedBlocksFilePath = fmt::format("{}/{}", blocksDirPath, "combined-block-list.json");

        if (fs::exists(combinedBlocksFilePath)) {
            logger.info(fmt::format("Skip combining blocks by date: {}", listFilePath));

            return;
        }

        std::ofstream combinedBlockFile(combinedBlocksFilePath);

        combinedBlockFile << "[";

        bool isFirstBlock = true;
        while (blockListFile) {
            std::string blockFilePath;
            std::getline(blockListFile, blockFilePath);

            if (blockFilePath.size() == 0) {
                break;
            }

            if (!isFirstBlock) {
                combinedBlockFile << ",";
            }
            else {
                isFirstBlock = false;
            }

            std::ifstream blockFile(blockFilePath.c_str(), std::ios::binary);
            utils::copyStream(blockFile, combinedBlockFile);
            blockFile.close();

            fs::remove(blockFilePath);
        }

        combinedBlockFile << "]";

        logger.info(fmt::format("Finished combining blocks by date: {}", listFilePath));
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when combining blocks by date: {}", listFilePath));
        logger.error(e.what());
    }
}