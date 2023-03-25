#include "btc-config.h"
#include "btc_gen_address/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/mem_utils.h"
#include "utils/btc_utils.h"
#include "fmt/format.h"
#include <argparse/argparse.hpp>

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

using json = nlohmann::json;

namespace fs = std::filesystem;


static argparse::ArgumentParser createArgumentParser();

void getUniqueAddressesOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    std::set<std::string>* inputAdresses,
    std::set<std::string>* outputAdresses
);

void getUniqueAddressesOfDay(
    const std::string& dayDir,
    std::set<std::string>& inputAdresses,
    std::set<std::string>& outputAdresses
);

inline void getUniqueAddressesOfBlock(
    const std::string& dayDir,
    const json& block,
    std::set<std::string>& inputAdresses,
    std::set<std::string>& outputAdresses
);

inline void getUniqueAddressesOfTx(
    const std::string& dayDir,
    const json& tx,
    std::set<std::string>& inputAdresses,
    std::set<std::string>& outputAdresses
);

inline std::set<std::string> mergeUniqueAddresses(
    const std::string& label,
    std::vector<std::set<std::string>>& tasksUniqueAddresses
);

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    auto argumentParser = createArgumentParser();

    std::string daysListFilePath = argumentParser.get("days_dir_list");
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));

    std::string id2AddressFilePath = argumentParser.get("id2addr");

    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    uint32_t workerCount = std::min(
        argumentParser.get<uint32_t>("--worker_count"),
        std::thread::hardware_concurrency()
    );
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);
    std::vector<std::set<std::string>> tasksInputUniqueAddresses(workerCount);
    std::vector<std::set<std::string>> tasksOutputUniqueAddresses(workerCount);

    uint32_t workerIndex = 0;
    std::vector<std::future<void>> tasks;
    for (const auto& taskChunk : taskChunks) {
        auto& taskInputUniqueAddresses = tasksInputUniqueAddresses[workerIndex];
        auto& taskOutputUniqueAddresses = tasksOutputUniqueAddresses[workerIndex];
        tasks.push_back(
            std::async(
                getUniqueAddressesOfDays,
                workerIndex,
                &taskChunk,
                &taskInputUniqueAddresses,
                &taskOutputUniqueAddresses
            )
        );

        ++ workerIndex;
    }
    utils::waitForTasks(logger, tasks);

    std::vector<std::set<std::string>> allUniqueAddresses;
    allUniqueAddresses.push_back(mergeUniqueAddresses("input", tasksInputUniqueAddresses));
    logUsedMemory();
    allUniqueAddresses.push_back(mergeUniqueAddresses("output", tasksOutputUniqueAddresses));
    logUsedMemory();

    const auto& id2Address = mergeUniqueAddresses("final", allUniqueAddresses);
    logUsedMemory();

    logger.info(fmt::format("Dump address to {}", id2AddressFilePath));
    utils::btc::dumpId2Address(id2AddressFilePath.c_str(), id2Address);
    logUsedMemory();

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_gen_address");

    program.add_argument("days_dir_list")
        .required()
        .help("List file path of days directories");

    program.add_argument("id2addr")
        .required()
        .help("Output file path of id2addr");

    program.add_argument("--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    return program;
}

void getUniqueAddressesOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysList,
    std::set<std::string>* inputAdresses,
    std::set<std::string>* outputAdresses
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    for (const auto& dayDir : *daysList) {
        getUniqueAddressesOfDay(dayDir, *inputAdresses, *outputAdresses);
        auto usedMemory = utils::mem::getAllocatedMemory();
        logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
    }
}

void getUniqueAddressesOfDay(
    const std::string& dayDir,
    std::set<std::string>& inputAdresses,
    std::set<std::string>& outputAdresses
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
            getUniqueAddressesOfBlock(dayDir, block, inputAdresses, outputAdresses);
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
    std::set<std::string>& inputAdresses,
    std::set<std::string>& outputAdresses
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        const auto& txs = block["tx"];

        for (const auto& tx : txs) {
            getUniqueAddressesOfTx(dayDir, tx, inputAdresses, outputAdresses);
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
    std::set<std::string>& inputAdresses,
    std::set<std::string>& outputAdresses
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
                inputAdresses.insert(addrItem.value());
            }
        }

        const auto& outputs = utils::json::get(tx, "out");
        for (const auto& output : outputs) {
            const auto addrItem = output.find("addr");
            if (addrItem != output.cend()) {
                outputAdresses.insert(addrItem.value());
            }
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}:{}", dayDir, txHash));
        logger.error(e.what());
    }
}

inline std::set<std::string> mergeUniqueAddresses(
    const std::string& label,
    std::vector<std::set<std::string>>& tasksUniqueAddresses
) {
    std::set<std::string> finalAddressSet;
    size_t totalAddressCount = 0;
    while (tasksUniqueAddresses.size()) {
        const auto& taskUniqueAddresses = tasksUniqueAddresses.back();
        logger.info(fmt::format("Generated task unique addresses: {}", taskUniqueAddresses.size()));
        totalAddressCount += taskUniqueAddresses.size();

        finalAddressSet.insert(taskUniqueAddresses.cbegin(), taskUniqueAddresses.cend());
        tasksUniqueAddresses.pop_back();
    }

    logger.info(fmt::format("Final unique addresses {}: {}/{}", label, finalAddressSet.size(), totalAddressCount));
    logger.info(fmt::format("Remove duplicated addresses {}: {}", label, totalAddressCount - finalAddressSet.size()));

    return finalAddressSet;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}
