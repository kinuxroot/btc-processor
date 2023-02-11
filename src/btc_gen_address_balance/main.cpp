#include "btc-config.h"
#include "btc_gen_address_balance/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/btc_utils.h"
#include "utils/mem_utils.h"
#include "fmt/format.h"
#include <nlohmann/json.hpp>

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

using json = nlohmann::json;
using BalanceList = std::vector<int64_t>;
using BalanceListPtr = std::shared_ptr<BalanceList>;

namespace fs = std::filesystem;

inline BtcId parseMaxId(const char* maxIdArg);

std::vector<
    std::pair<std::string, std::vector<std::string>>
> groupDaysListByYears(const std::vector<std::string>& daysList);

BalanceListPtr generateBalanceListOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    BtcId maxSize
);

void calculateBalanceListOfDays(
    const std::string& dayDir,
    BalanceListPtr balanceList
);

void calculateBalanceListOfBlock(
    const json& block,
    BalanceListPtr balanceList
);

void calculateBalanceListOfTx(
    const json& tx,
    BalanceListPtr balanceList
);

void mergeBalanceList(
    BalanceList& dest,
    const BalanceList& src
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
            "Usage: btc_gen_address_balance <days_dir_list> <id_max_value> <output_base_dir>\n" 
            << std::endl;

        return EXIT_FAILURE;
    }

    const char* daysListFilePath = argv[1];
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    std::vector<std::string> daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    auto groupedDaysList = groupDaysListByYears(daysList);

    BtcId maxId = parseMaxId(argv[2]);
    if (!maxId) {
        return EXIT_FAILURE;
    }

    uint32_t workerCount = std::min(BTC_GEN_ADDRESS_BALANCE_WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    logUsedMemory();

    const char* outputBaseDirPath = argv[3];
    fs::create_directories(outputBaseDirPath);

    BalanceList balanceList(maxId, 0);
    for (const auto& yearDaysList : groupedDaysList) {
        const auto& year = yearDaysList.first;

        logger.info(fmt::format("\n\n======================== Process year: {} ========================\n", year));
        const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(
            yearDaysList.second, workerCount
        );
        uint32_t workerIndex = 0;
        std::vector<std::future<BalanceListPtr>> tasks;

        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(
                std::async(generateBalanceListOfDays, workerIndex, &taskChunk, maxId)
            );

            ++workerIndex;
        }

        for (auto& task : tasks) {
            BalanceListPtr taskResult = std::move(task.get());
            mergeBalanceList(balanceList, *taskResult);

            taskResult.reset();
        }

        auto outputFilePath = fmt::format("{}/{}.list", outputBaseDirPath, year);
        dumpBalanceList(outputFilePath, balanceList);
    }

    return EXIT_SUCCESS;
}

std::vector<std::pair<std::string, std::vector<std::string>>>
groupDaysListByYears(const std::vector<std::string>& daysList) {
    std::map<std::string, std::vector<std::string>> yearDaysListMap;
    
    for (const auto& dayDirPathLine : daysList) {
        fs::path dayDirPath(dayDirPathLine);

        const std::string& dirName = dayDirPath.filename().string();
        const auto& year = dirName.substr(0, 4);
        const auto& yearDaysListIt = yearDaysListMap.find(year);
        if (yearDaysListIt == yearDaysListMap.end()) {
            yearDaysListMap[year] = std::vector<std::string>{ dayDirPathLine };
        }
        else {
            yearDaysListIt->second.push_back(dayDirPathLine);
        }
    }

    std::vector<
        std::pair<std::string, std::vector<std::string>>
    > groupedDaysList(yearDaysListMap.begin(), yearDaysListMap.end());

    std::sort(groupedDaysList.begin(), groupedDaysList.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });

    return groupedDaysList;
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

BalanceListPtr generateBalanceListOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    BtcId maxId
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    BalanceListPtr balanceList = std::make_shared<BalanceList>(maxId, 0);
    for (const auto& dayDir : *daysDirList) {
        calculateBalanceListOfDays(dayDir, balanceList);
    }

    return balanceList;
}

void calculateBalanceListOfDays(
    const std::string& dayDir,
    BalanceListPtr balanceList
) {
    try {
        auto convertedBlocksFilePath = fmt::format("{}/{}", dayDir, "converted-block-list.json");
        logger.info(fmt::format("Process combined blocks file: {}", dayDir));

        std::ifstream convertedBlocksFile(convertedBlocksFilePath.c_str());
        if (!convertedBlocksFile.is_open()) {
            logger.warning(fmt::format("Skip processing blocks by date because file not exists: {}", convertedBlocksFilePath));
            return;
        }

        logUsedMemory();
        json blocks;
        convertedBlocksFile >> blocks;
        logger.info(fmt::format("Block count: {} {}", dayDir, blocks.size()));
        logUsedMemory();

        for (const auto& block : blocks) {
            calculateBalanceListOfBlock(block, balanceList);
        }

        logUsedMemory();

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

void calculateBalanceListOfBlock(
    const json& block,
    BalanceListPtr balanceList
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        const auto& txs = block["tx"];

        for (const auto& tx : txs) {
            calculateBalanceListOfTx(tx, balanceList);
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}", blockHash));
        logger.error(e.what());
    }
}

void calculateBalanceListOfTx(
    const json& tx,
    BalanceListPtr balanceList
) {
    std::string txHash = utils::json::get(tx, "hash");

    try {
        const auto& inputs = utils::json::get(tx, "inputs");
        for (const auto& input : inputs) {
            const auto prevOutItem = input.find("prev_out");
            if (prevOutItem == input.cend()) {
                continue;
            }
            const auto& prevOut = prevOutItem.value();

            const auto addrItem = prevOut.find("addr");
            if (addrItem == prevOut.cend()) {
                continue;
            }
            BtcId addressId = addrItem.value();

            const auto valueItem = input.find("value");
            if (valueItem == input.cend()) {
                continue;
            }
            int64_t value = valueItem.value();

            balanceList->at(addressId) -= value;
        }

        auto& outputs = utils::json::get(tx, "out");
        for (auto& output : outputs) {
            auto addrItem = output.find("addr");
            if (addrItem == output.cend()) {
                continue;
            }
            BtcId addressId = addrItem.value();

            const auto valueItem = output.find("value");
            if (valueItem == output.cend()) {
                continue;
            }
            int64_t value = valueItem.value();

            balanceList->at(addressId) += value;
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}", txHash));
        logger.error(e.what());
    }
}

void mergeBalanceList(
    BalanceList& dest,
    const BalanceList& src
) {
    auto destIt = dest.begin();
    auto srcIt = src.begin();

    while (destIt != dest.end() && srcIt != dest.end()) {
        *destIt += *srcIt;

        ++destIt;
        ++srcIt;
    }
}

void dumpBalanceList(
    const std::string& outputFilePath,
    const BalanceList& balanceList
) {
    std::ofstream outputFile(outputFilePath.c_str());

    for (auto balance : balanceList) {
        outputFile << balance << std::endl;
    }
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}