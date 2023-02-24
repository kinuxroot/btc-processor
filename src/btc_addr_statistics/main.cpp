#include "btc-config.h"
#include "btc_addr_statistics/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/mem_utils.h"
#include "utils/btc_utils.h"
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
#include <stdexcept>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
using AddressOutputCounts = std::vector<uint64_t>;

struct AddressOutputStatistics {
    AddressOutputStatistics() = default;
    AddressOutputStatistics(BtcId maxId) : txCounts(maxId, 0), inputCounts(maxId, 0) {}

    AddressOutputStatistics(const AddressOutputStatistics&) = delete;
    AddressOutputStatistics(const AddressOutputStatistics&&) = delete;

    AddressOutputStatistics& operator=(const AddressOutputStatistics&) = delete;
    AddressOutputStatistics& operator=(const AddressOutputStatistics&&) = delete;

    AddressOutputCounts txCounts;
    AddressOutputCounts inputCounts;
};

using AddressOutputStatisticsPtr = std::unique_ptr<AddressOutputStatistics>;

namespace fs = std::filesystem;

AddressOutputStatisticsPtr getAddressOutputStatisticsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    BtcId maxId
);

void getAddressOutputStatisticsOfDay(
    const std::string& dayDir,
    AddressOutputStatistics& addressOutputStatistics
);

void getAddressOutputStatisticsOfBlock(
    const std::string& dayDir,
    const json& block,
    AddressOutputStatistics& addressOutputStatistics,
    const std::string& filePath,
    std::size_t blockOffset
);

std::set<BtcId> getAddressOutputsOfTx(const std::string& dayDir, json tx);

std::size_t getAddressInputsOfTx(const std::string& dayDir, json tx);

AddressOutputStatisticsPtr mergeAddressOutputStatisticsList(
    std::vector<AddressOutputStatisticsPtr>& addressOutputStatisticsList
);

void dumpAddressOutputStatistics(
    const fs::path& filePath,
    const AddressOutputStatistics& addressOutputStatistics
);

template <typename T>
void dumpAddressOutputCounts(
    const fs::path& filePath,
    const std::vector<T>& outputCounts
);

inline BtcId parseMaxId(const char* maxIdArg);
inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_addr_statistics <days_dir_list> <id_max_value> <out_file>\n" << std::endl;

        return EXIT_FAILURE;
    }

    const char* daysListFilePath = argv[1];
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    BtcId maxId = parseMaxId(argv[2]);
    if (!maxId) {
        return EXIT_FAILURE;
    }

    uint32_t workerCount = std::min(BTC_ADDR_STATISTICS_WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);
    std::vector<std::set<BtcId>> tasksUniqueAddresses(workerCount);

    uint32_t workerIndex = 0;
    std::vector<std::future<AddressOutputStatisticsPtr>> tasks;
    for (const auto& taskChunk : taskChunks) {
        auto& taskUniqueeAddresses = tasksUniqueAddresses[workerIndex];
        tasks.push_back(
            std::async(getAddressOutputStatisticsOfDays, workerIndex, &taskChunk, maxId)
        );

        ++workerIndex;
    }

    std::vector<AddressOutputStatisticsPtr> addressOutputStatisticsPtrList;
    for (auto& task : tasks) {
        addressOutputStatisticsPtrList.push_back(task.get());
    }

    auto mergedAddressOutputCounts = mergeAddressOutputStatisticsList(addressOutputStatisticsPtrList);
    const char* mergedAddressOutputCountsFilePath = argv[3];
    dumpAddressOutputStatistics(mergedAddressOutputCountsFilePath, *mergedAddressOutputCounts);

    return EXIT_SUCCESS;
}

AddressOutputStatisticsPtr getAddressOutputStatisticsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    BtcId maxId
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    auto minerTxs = std::make_unique<AddressOutputStatistics>(maxId);
    for (const auto& dayDir : *daysDirList) {
        getAddressOutputStatisticsOfDay(dayDir, *minerTxs);
        logUsedMemory();
    }

    return minerTxs;
}

void getAddressOutputStatisticsOfDay(
    const std::string& dayDir,
    AddressOutputStatistics& addressOutputStatistics
) {
    try {
        auto convertedBlocksFilePath = fmt::format("{}/{}", dayDir, "converted-block-list.json");
        logger.info(fmt::format("Process combined blocks file: {}", dayDir));

        std::ifstream convertedBlocksFile(convertedBlocksFilePath.c_str());
        if (!convertedBlocksFile.is_open()) {
            logger.warning(fmt::format("Finished process blocks by date because file not exists: {}", convertedBlocksFilePath));
            return;
        }

        logUsedMemory();
        json blocks;
        convertedBlocksFile >> blocks;
        logger.info(fmt::format("Block count: {} {}", dayDir, blocks.size()));
        logUsedMemory();

        std::size_t blockOffset = 0;
        for (const auto& block : blocks) {
            getAddressOutputStatisticsOfBlock(dayDir, block, addressOutputStatistics, convertedBlocksFilePath, blockOffset);
            ++blockOffset;
        }

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

void getAddressOutputStatisticsOfBlock(
    const std::string& dayDir,
    const json& block,
    AddressOutputStatistics& addressOutputStatistics,
    const std::string& filePath,
    std::size_t blockOffset
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        const auto& txs = block["tx"];
        if (!txs.is_array()) {
            throw std::invalid_argument("tx must be an array");
        }

        if (!txs.size()) {
            throw std::invalid_argument("tx must have elements");
        }
        
        for (const auto& tx : txs) {
            auto inputCount = getAddressInputsOfTx(dayDir, tx);
            std::set<BtcId> outputIds = getAddressOutputsOfTx(dayDir, tx);

            for (BtcId outputId : outputIds) {
                ++ addressOutputStatistics.txCounts[outputId];
                addressOutputStatistics.inputCounts[outputId] += inputCount;
            }
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}:{}", dayDir, blockHash));
        logger.error(e.what());
    }
}

std::set<BtcId> getAddressOutputsOfTx(
    const std::string& dayDir,
    json tx
) {
    std::string txHash = utils::json::get(tx, "hash");
    std::set<BtcId> outputAddresses;

    try {
        auto outputsIt = tx.find("out");
        if (outputsIt == tx.end()) {
            return outputAddresses;
        }

        auto& outputs = outputsIt.value();
        for (auto& output : outputs) {
            const auto addrItem = output.find("addr");
            if (addrItem != output.cend()) {
                BtcId addressId = addrItem.value();
                outputAddresses.insert(addressId);
            }
        }

        return outputAddresses;
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process outputs of tx {}:{}", dayDir, txHash));
        logger.error(e.what());
    }

    return outputAddresses;
}

std::size_t getAddressInputsOfTx(
    const std::string& dayDir,
    json tx
) {
    std::string txHash = utils::json::get(tx, "hash");
    std::size_t inputCount = 0;

    try {
        const auto& inputs = utils::json::get(tx, "inputs");
        for (const auto& input : inputs) {
            const auto prevOutItem = input.find("prev_out");
            if (prevOutItem == input.cend()) {
                continue;
            }
            const auto& prevOut = prevOutItem.value();

            const auto addrItem = prevOut.find("addr");
            if (addrItem != prevOut.cend()) {
                BtcId addressId = addrItem.value();
                ++inputCount;
            }
        }

        return inputCount;
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process inputs of tx {}:{}", dayDir, txHash));
        logger.error(e.what());
    }

    return inputCount;
}

json toMinerTx(
    const std::string& dayDir,
    const json& tx
) {
    std::string txHash = utils::json::get(tx, "hash");

    try {
        const auto& outputs = utils::json::get(tx, "out");
        if (!outputs.is_array()) {
            throw std::invalid_argument("outputs must be an array");
        }

        if (!outputs.size()) {
            throw std::invalid_argument("outputs must have elements");
        }

        return outputs;
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}:{}", dayDir, txHash));
        logger.error(e.what());

        std::rethrow_exception(std::current_exception());
    }
}

AddressOutputStatisticsPtr mergeAddressOutputStatisticsList(
    std::vector<AddressOutputStatisticsPtr>& addressOutputStatisticsList
) {
    AddressOutputStatisticsPtr mergedOutputStatistics;

    while (addressOutputStatisticsList.size()) {
        auto& currentOutputStatistics = addressOutputStatisticsList.back();
        if (!mergedOutputStatistics) {
            mergedOutputStatistics = std::move(currentOutputStatistics);
        }
        else {
            for (BtcId currentId = 0; currentId != currentOutputStatistics->inputCounts.size(); ++currentId) {
                mergedOutputStatistics->inputCounts[currentId] += currentOutputStatistics->inputCounts[currentId];
            }

            for (BtcId currentId = 0; currentId != currentOutputStatistics->txCounts.size(); ++currentId) {
                mergedOutputStatistics->txCounts[currentId] += currentOutputStatistics->txCounts[currentId];
            }
        }

        addressOutputStatisticsList.pop_back();
    }

    return mergedOutputStatistics;
}

void dumpAddressOutputStatistics(
    const fs::path& filePath,
    const AddressOutputStatistics& addressOutputStatistics
) {
    logger.info(fmt::format("Dump AddressOutputStatistics: {}", filePath.string()));
    fs::create_directories(filePath);

    dumpAddressOutputCounts(filePath / "addr_tx_counts.list", addressOutputStatistics.txCounts);
    dumpAddressOutputCounts(filePath / "addr_input_counts.list", addressOutputStatistics.inputCounts);
    
    std::vector<double> addrTxInputAvgs(addressOutputStatistics.txCounts.size(), 0.0);
    for (BtcId currentId = 0; currentId != addrTxInputAvgs.size(); ++currentId) {
        if (!addressOutputStatistics.txCounts[currentId]) {
            continue;
        }

        addrTxInputAvgs[currentId] = static_cast<double>(addressOutputStatistics.inputCounts[currentId]) /
            static_cast<double>(addressOutputStatistics.txCounts[currentId]);
    }
    dumpAddressOutputCounts(filePath / "addr_input_avgs.list", addrTxInputAvgs);
}

template <typename T>
void dumpAddressOutputCounts(
    const fs::path& filePath,
    const std::vector<T>& outputCounts
) {
    logger.info(fmt::format("Dump outputCounts to: {}", filePath.string()));

    std::ofstream outputFile(filePath.c_str());

    BtcId btcId = 0;
    for (auto count : outputCounts) {
        outputFile << btcId << "," << count << std::endl;

        ++btcId;

        if (btcId % 100000 == 0) {
            logger.info(fmt::format("Dump output id: {}", btcId));
        }
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

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}