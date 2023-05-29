// 将文本格式的余额列表转换成二进制余额列表

#include "btc-config.h"
#include "btc_convert_entity_balance/logger.h"

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
#include <filesystem>
#include <thread>
#include <iostream>

using BalanceValue = double;
using BalanceList = std::vector<BalanceValue>;
using BalanceListPtr = std::shared_ptr<BalanceList>;

namespace fs = std::filesystem;

inline BtcId parseMaxId(const char* maxIdArg);
std::vector<std::string> getAddressBalancePaths(const std::string& dirPath);
void convertBalanceListFiles(
    uint32_t workerIndex,
    const std::vector<std::string>* filePaths,
    BtcId maxId
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
    if (argc < 3) {
        std::cerr << "Invalid arguments!\n\n"
            << "Usage: btc_convert_entity_balance <output_base_dir> <id_max_value>"
            << std::endl;

        return EXIT_FAILURE;
    }

    try {
        const char* outputBaseDirPath = argv[1];

        uint32_t workerCount = std::min(BTC_CONVERT_ADDRESS_BALANCE_WORKER_COUNT, std::thread::hardware_concurrency());
        logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
        logger.info(fmt::format("Worker count: {}", workerCount));

        auto addressBalancePaths = getAddressBalancePaths(outputBaseDirPath);
        const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(
            addressBalancePaths, workerCount
        );

        BtcId maxId = parseMaxId(argv[2]);
        if (!maxId) {
            return EXIT_FAILURE;
        }
        logUsedMemory();

        uint32_t workerIndex = 0;
        std::vector<std::future<void>> tasks;
        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(
                std::async(convertBalanceListFiles, workerIndex, &taskChunk, maxId)
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
    const std::vector<std::string>* filePaths,
    BtcId maxId
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    for (const auto& filePath : *filePaths) {
        BalanceList balanceList(maxId, 0);
        
        loadBalanceList(filePath, balanceList);
        logUsedMemory();

        std::string outputFilePath = filePath;
        std::string ext = ".list";
        std::size_t extPos = outputFilePath.rfind(ext);
        outputFilePath.replace(extPos, extPos + ext.size(), ".out");
        if (filePath == outputFilePath) {
            logger.error(fmt::format("Path must be different {}:{}", filePath, outputFilePath));

            continue;
        }
        dumpBalanceList(outputFilePath, balanceList);
        logUsedMemory();
    }
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

std::vector<std::string> splitString(const std::string& text, char seperator) {
    std::size_t startPos = 0;
    std::vector<std::string> segments;

    while (startPos < text.size()) {
        auto seperatorPos = text.find(',', startPos);
        if (seperatorPos == std::string::npos) {
            segments.push_back(text.substr(startPos));

            break;
        }

        segments.push_back(text.substr(startPos, seperatorPos - startPos));
        startPos = seperatorPos + 1;
    }

    return segments;
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

        const auto& parts = splitString(line, ',');
        std::string btcIdString = parts[0];
        std::string balanceString = parts[2];

        BtcId btcId = std::stoll(btcIdString);
        BalanceValue btcValue = std::stod(balanceString);

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
    outputFile.write(reinterpret_cast<const char*>(balanceList.data()), sizeof(BalanceValue) * balanceSize);
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}