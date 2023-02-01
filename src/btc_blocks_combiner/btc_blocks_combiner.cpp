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

namespace fs = std::filesystem;

std::vector<std::string> readDaysList(const char* daysListFilePath);
void combineBlocksFromList(const std::string& listFilePath);

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_combine_blocks <days_list>\n" << std::endl;

        return EXIT_FAILURE;
    }

    const std::vector<std::string>& daysList = readDaysList(argv[1]);
    for (const auto& dayListPath : daysList) {
        combineBlocksFromList(dayListPath);
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
