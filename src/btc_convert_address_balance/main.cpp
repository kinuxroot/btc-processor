#include "btc-config.h"
#include "btc_convert_address_balance/logger.h"

#include "logging/Logger.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/btc_utils.h"
#include "utils/mem_utils.h"
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
#include <iostream>
#include <map>

using BalanceList = std::vector<int64_t>;
using BalanceListPtr = std::shared_ptr<BalanceList>;

namespace fs = std::filesystem;

std::vector<std::string> getAddressBalancePaths(const std::string& dirPath);
void convertBalanceListFiles(
    uint32_t workerIndex,
    const std::vector<std::string>* filePaths
);

std::size_t loadBalanceList(
    const std::string& inputFilePath,
    BalanceList& balanceList
);

void dumpBalanceList(
    const std::string& outputFilePath,
    const BalanceList& balanceList
);

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Invalid arguments!\n\n"
            << "Usage: btc_filter_address_balance <output_base_dir> <start_year> <end_year>"
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

        uint32_t workerCount = std::min(BTC_CONVERT_ADDRESS_BALANCE_WORKER_COUNT, std::thread::hardware_concurrency());
        logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
        logger.info(fmt::format("Worker count: {}", workerCount));

        auto addressBalancePaths = getAddressBalancePaths(outputBaseDirPath);
        const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(
            addressBalancePaths, workerCount
        );

        uint32_t workerIndex = 0;
        std::vector<std::future<void>> tasks;
        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(
                std::async(convertBalanceListFiles, workerIndex, &taskChunk)
            );

            ++workerIndex;
        }
        logUsedMemory();

        utils::waitForTasks(logger, tasks);
        logUsedMemory();
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

std::vector<std::string> getAddressBalancePaths(const std::string& dirPath) {
    std::vector<std::string> addressBalanceFilePaths;

    for (auto const& dirEntry : std::filesystem::directory_iterator{ dirPath })
    {
        if (dirEntry.is_directory()) {
            continue;
        }

        const auto& filePath = dirEntry.path();
        if (!filePath.has_extension()) {
            continue;
        }

        const auto fileExtension = filePath.extension().string();
        if (fileExtension.find("list") == std::string::npos) {
            continue;
        }
        
        addressBalanceFilePaths.push_back(filePath.string());
    }

    return addressBalanceFilePaths;
}

void convertBalanceListFiles(
    uint32_t workerIndex,
    const std::vector<std::string>* filePaths
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    for (const auto& filePath : *filePaths) {
        BalanceList balanceList;

        loadBalanceList(filePath, balanceList);
        logUsedMemory();
        dumpBalanceList(filePath, balanceList);
        logUsedMemory();
    }
}

std::size_t loadBalanceList(
    const std::string& inputFilePath,
    BalanceList& balanceList
) {
    logger.info(fmt::format("Load balance from: {}", inputFilePath));
    std::ifstream inputFile(inputFilePath.c_str());
    std::size_t loadedCount = 0;

    while (inputFile) {
        std::string line;
        std::getline(inputFile, line);

        auto seperatorPos = line.find(',');
        if (seperatorPos == std::string::npos) {
            continue;
        }

        std::string btcIdString = line.substr(0, seperatorPos);
        std::string balanceString = line.substr(seperatorPos + 1);

        BtcId btcId = std::stoll(btcIdString);
        int64_t btcValue = std::stoll(balanceString);

        balanceList[btcId] = btcValue;
        ++ loadedCount;
    }

    return loadedCount;
}

void dumpBalanceList(
    const std::string& outputFilePath,
    const BalanceList& balanceList
) {
    logger.info(fmt::format("Dump balance to: {}", outputFilePath));

    std::ofstream outputFile(outputFilePath.c_str(), std::ios::binary);

    BtcId btcId = 0;
    std::size_t balanceSize = balanceList.size();
    outputFile.write(reinterpret_cast<char*>(&balanceSize), sizeof(balanceSize));
    outputFile.write(reinterpret_cast<const char*>(balanceList.data()), sizeof(balanceSize) * balanceSize);
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}