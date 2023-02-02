#include "btc-config.h"
#include "btc_blocks_combiner/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
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
#include <nlohmann/json.hpp>

#ifdef __GNUC__
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif //__GNUC__

using json = nlohmann::json;

namespace fs = std::filesystem;
uint32_t WORKER_COUNT = 16;

void getUniqueAddressesOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    std::set<std::string>* addresses
);

void getUniqueAddressesOfDay(
    const std::string& dayDir,
    std::set<std::string>& addresses
);

inline void getUniqueAddressesOfBlock(
    const std::string& dayDir,
    const json& block,
    std::set<std::string>& addresses
);

inline void getUniqueAddressesOfTx(
    const std::string& dayDir,
    const json& tx,
    std::set<std::string>& addresses
);

inline std::set<std::string> mergeUniqueAddresses(
    const std::vector<std::set<std::string>>& tasksUniqueAddresses
);

inline std::map<std::string, std::size_t> generateAddress2Id(const std::vector<std::string>& id2Address);

inline void dumpId2Address(const char* filePath, const std::vector<std::string>& id2Address);

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_combine_blocks <days_dir_list> <id2addr> <addr2id>\n" << std::endl;

        return EXIT_FAILURE;
    }

    const char* daysListFilePath = argv[1];
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    uint32_t workerCount = std::min(WORKER_COUNT, std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, WORKER_COUNT);
    std::vector<std::set<std::string>> tasksUniqueAddresses(workerCount);

    uint32_t workerIndex = 0;
    std::vector<std::future<void>> tasks;
    for (const auto& taskChunk : taskChunks) {
        auto& taskUniqueeAddresses = tasksUniqueAddresses[workerIndex];
        tasks.push_back(
            std::async(getUniqueAddressesOfDays, workerIndex, &taskChunk, &taskUniqueeAddresses)
        );

        ++ workerIndex;
    }
    utils::waitForTasks(logger, tasks);

    const auto& finalUniqueAddress = mergeUniqueAddresses(tasksUniqueAddresses);

    std::vector<std::string> id2Address(finalUniqueAddress.begin(), finalUniqueAddress.end());
    const char* id2AddressFilePath = argv[2];
    dumpId2Address(id2AddressFilePath, id2Address);

    return EXIT_SUCCESS;
}

void getUniqueAddressesOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysList,
    std::set<std::string>* addresses
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    rusage rup;
    for (const auto& dayDir : *daysList) {
        getUniqueAddressesOfDay(dayDir, *addresses);
#ifdef __GNUC__
        getrusage(RUSAGE_SELF, &rup);
        logger.debug(fmt::format("Use memory: {}MB {}B", rup.ru_maxrss, rup.ru_maxrss / 1024 / 1024));
#endif //__GNUC__
    }
}

void getUniqueAddressesOfDay(
    const std::string& dayDir,
    std::set<std::string>& addresses
) {
    try {
        auto combinedBlocksFilePath = fmt::format("{}/{}", dayDir, "combined-block-list.json");
        logger.info(fmt::format("Process combined blocks file: {}", dayDir));

        std::ifstream combinedBlocksFile(combinedBlocksFilePath.c_str());
        if (!combinedBlocksFile.is_open()) {
            logger.warning(fmt::format("Finished process blocks by date because file not exists: {}", combinedBlocksFilePath));
            return;
        }

        json blocks;
        combinedBlocksFile >> blocks;
        logger.info(fmt::format("Block count: {} {}", dayDir, blocks.size()));

        for (const auto& block : blocks) {
            getUniqueAddressesOfBlock(dayDir, block, addresses);
        }

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

inline void getUniqueAddressesOfBlock(
    const std::string& dayDir,
    const json& block,
    std::set<std::string>& addresses
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        const auto& txs = block["tx"];

        for (const auto& tx : txs) {
            getUniqueAddressesOfTx(dayDir, tx, addresses);
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}:{}", dayDir, blockHash));
        logger.error(e.what());
    }
}

inline void getUniqueAddressesOfTx(
    const std::string& dayDir,
    const json& tx,
    std::set<std::string>& addresses
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
            if (addrItem != prevOut.cend()) {
                addresses.insert(addrItem.value());
            }
        }

        const auto& outputs = utils::json::get(tx, "out");
        for (const auto& output : outputs) {
            const auto addrItem = output.find("addr");
            if (addrItem != output.cend()) {
                addresses.insert(addrItem.value());
            }
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}:{}", dayDir, txHash));
        logger.error(e.what());
    }
}

inline std::set<std::string> mergeUniqueAddresses(
    const std::vector<std::set<std::string>>& tasksUniqueAddresses
) {
    std::set<std::string> finalAddressSet;
    size_t totalAddressCount = 0;
    for (const auto& taskUniqueAddresses : tasksUniqueAddresses) {
        logger.info(fmt::format("Generated task unique addresses: {}", taskUniqueAddresses.size()));
        totalAddressCount += taskUniqueAddresses.size();

        finalAddressSet.insert(taskUniqueAddresses.cbegin(), taskUniqueAddresses.cend());
    }

    logger.info(fmt::format("Final unique addresses: {}/{}", finalAddressSet.size(), totalAddressCount));
    logger.info(fmt::format("Remove duplicated addresses: {}", totalAddressCount - finalAddressSet.size()));

    return finalAddressSet;
}

inline std::map<std::string, std::size_t> generateAddress2Id(const std::vector<std::string>& id2Address) {
    std::map<std::string, std::size_t> address2Id;

    size_t addressId = 0;
    for (const auto& address : id2Address) {
        address2Id[address] = addressId;

        ++ addressId;
    }

    return address2Id;
}

inline void dumpId2Address(const char* filePath, const std::vector<std::string>& id2Address) {
    logger.info(fmt::format("Dump i2daddr: {}", filePath));

    json id2AddressJson(id2Address);
    std::ofstream id2AddressFile(filePath);
    id2AddressFile << id2AddressJson;
}
