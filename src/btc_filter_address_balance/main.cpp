#include "btc-config.h"
#include "btc_gen_day_ins/logger.h"

#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "utils/io_utils.h"
#include "fmt/format.h"

#include <cstdlib>
#include <iostream>

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Invalid arguments!\n\n"
            << "Usage: btc_filter_address_balance <output_base_dir> <start_year> <end_year> <exclusive_file>"
            << std::endl;

        return EXIT_FAILURE;
    }

    try {
        const char* outputBaseDirPath = argv[1];
        
        int32_t startYear = std::stoi(argv[2]);
        logger.info(fmt::format("Using start year: {}", startYear));

        int32_t endYear = std::stoi(argv[3]);
        logger.info(fmt::format("Using end year: {}", endYear));

        std::vector<std::string> allYears;
        for (int32_t year = startYear; year != endYear; ++year) {
            allYears.push_back(std::to_string(year));
        }
        logger.info(fmt::format("Generate years count: {}", allYears.size()));

        uint32_t workerCount = std::min(BTC_FILTER_ADDRESS_BALANCE_WORKER_COUNT, std::thread::hardware_concurrency());
        logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
        logger.info(fmt::format("Worker count: {}", workerCount));

        const char* exclusiveFilePath = argv[4];
        std::vector<BtcId> exclusiveAddressIds = utils::readLines<std::vector<BtcId>, BtcId>(
            exclusiveFilePath,
            [](const std::string& line) -> BtcId {
                return std::stoi(line);
            }
        );


    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}