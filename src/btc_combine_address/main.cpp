#include "btc-config.h"
#include "btc_combine_blocks/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/btc_utils.h"
#include "fmt/format.h"

#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string>
#include <set>
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;

void combineBlocksOfDays(uint32_t workerIndex, const std::vector<std::string>& daysList);
void combineBlocksFromList(const fs::path& dayDirPath);

auto& logger = getLogger();

int main(int32_t argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_combine_address <combined_id2addr> <id2addrs>\n" << std::endl;

        return EXIT_FAILURE;
    }
    
    try {
        std::set<std::string> id2Addr;
        int32_t inputFileCount = argc - 2;
        logger.info(fmt::format("List file count: {}", inputFileCount));
        for (int32_t inputFileIndex = 0; inputFileIndex != inputFileCount; ++inputFileIndex) {
            const char* id2addrFilePath = argv[inputFileIndex + 2];
            logger.info(fmt::format("Merge id2addr form {}", id2addrFilePath));

            utils::readLines(id2addrFilePath, id2Addr);
        }
        logger.info(fmt::format("Merge address count: {}", id2Addr.size()));

        const char* combinedFilePath = argv[1];
        logger.info(fmt::format("Dump merged file to: {}", combinedFilePath));
        utils::btc::dumpId2Address(combinedFilePath, id2Addr);
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}