#include "btc_gen_day_ins/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/btc_utils.h"
#include "utils/mem_utils.h"
#include "fmt/format.h"
#include <nlohmann/json.hpp>
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
#include <iostream>

using json = nlohmann::json;

namespace fs = std::filesystem;

static argparse::ArgumentParser createArgumentParser();

void generateTxInputsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysDirList,
    const std::set<BtcId>* excludeAddresses,
    bool skipExisted,
    std::string dayInputsFileName
);

void generateTxInputsOfDay(
    const std::string& dayDir,
    const std::set<BtcId>& excludeAddresses,
    bool skipExisted,
    std::string dayInputsFileName
);

inline std::vector<std::vector<BtcId>> generateTxInputsOfBlock(
    const std::string& dayDir,
    const std::set<BtcId>& excludeAddresses,
    const json& block
);

inline std::vector<BtcId> generateTxInputs(
    const std::string& dayDir,
    const std::set<BtcId>& excludeAddresses,
    const json& tx
);

inline void dumpDayInputs(
    const char* filePath,
    const std::vector<std::vector<std::vector<BtcId>>>& txInputsOfDay
);

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    auto argumentParser = createArgumentParser();
    try {
        argumentParser.parse_args(argc, argv);
    }
    catch (const std::runtime_error& err) {
        logger.error(err.what());
        std::cerr << argumentParser;
        std::exit(1);
    }

    std::string daysListFilePath = argumentParser.get("days_dir_list");
    logger.info(fmt::format("Read tasks form {}", daysListFilePath));
    const std::vector<std::string>& daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    uint32_t workerCount = std::min(argumentParser.get<uint32_t>("--worker_count"), std::thread::hardware_concurrency());
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    logUsedMemory();

    const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(daysList, workerCount);

    logUsedMemory();

    std::set<BtcId> excludeAddresses;
    const std::string excludeAddressListFilePath = argumentParser.get("--exclude_addrs");
    if (!excludeAddressListFilePath.empty()) {
        logger.info(fmt::format("Load excludeAddresses: {}", excludeAddressListFilePath));
        excludeAddresses = utils::readLines<std::set<BtcId>, BtcId>(
            excludeAddressListFilePath,
            [](const std::string& line) -> BtcId {
                return std::stoi(line);
            },
            [](std::set<BtcId>& container, BtcId addressId) {
                container.insert(addressId);
            }
        );
        logger.info(fmt::format("Loaded excludeAddresses: {}", excludeAddresses.size()));
        logUsedMemory();
    }

    bool skipExisted = argumentParser.get<bool>("--skip_existed");
    if (skipExisted) {
        logger.info("Skip completed tasks");
    }

    std::string dayInputsFileName = argumentParser.get("--day_ins_file");
    uint32_t workerIndex = 0;
    std::vector<std::future<void>> tasks;
    for (const auto& taskChunk : taskChunks) {
        tasks.push_back(
            std::async(generateTxInputsOfDays, workerIndex, &taskChunk, &excludeAddresses, skipExisted, dayInputsFileName)
        );

        ++workerIndex;
    }
    utils::waitForTasks(logger, tasks);

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_gen_day_ins");

    program.add_argument("days_dir_list")
        .required()
        .help("List file path of days directories");

    program.add_argument("-d", "--day_ins_file")
        .help("Filename of day inputs address file")
        .default_value("day-inputs.json");

    program.add_argument("-e", "--exclude_addrs")
        .help("Exclude addresses file path")
        .default_value("");

    program.add_argument("-s", "--skip_existed")
        .help("Skip existed file")
        .implicit_value(true)
        .default_value(false);

    program.add_argument("-w", "--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    return program;
}

void generateTxInputsOfDays(
    uint32_t workerIndex,
    const std::vector<std::string>* daysList,
    const std::set<BtcId>* excludeAddresses,
    bool skipExisted,
    std::string dayInputsFileName
) {
    logger.info(fmt::format("Worker started: {}", workerIndex));

    for (const auto& dayDir : *daysList) {
        generateTxInputsOfDay(dayDir, *excludeAddresses, skipExisted, dayInputsFileName);
    }
}

void generateTxInputsOfDay(
    const std::string& dayDir,
    const std::set<BtcId>& excludeAddresses,
    bool skipExisted,
    std::string dayInputsFileName
) {
    try {
        auto txInputsOfDayFilePath = fmt::format("{}/{}", dayDir, dayInputsFileName);
        if (skipExisted && fs::exists(txInputsOfDayFilePath)) {
            logger.info(fmt::format("Skip existed blocks by date: {}", dayDir));

            return;
        }

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

        std::vector<std::vector<std::vector<BtcId>>> txInputsOfDay;
        for (const auto& block : blocks) {
            const auto& txInputsOfBlock = generateTxInputsOfBlock(dayDir, excludeAddresses, block);
            if (txInputsOfBlock.size() > 0) {
                txInputsOfDay.push_back(txInputsOfBlock);
            }
        }

        logger.info(fmt::format("Dump tx inputs of {} blocks by date: {}", txInputsOfDay.size(), dayDir));

        logUsedMemory();
        dumpDayInputs(txInputsOfDayFilePath.c_str(), txInputsOfDay);

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());
    }
}

inline std::vector<std::vector<BtcId>> generateTxInputsOfBlock(
    const std::string& dayDir,
    const std::set<BtcId>& excludeAddresses,
    const json& block
) {
    std::string blockHash = utils::json::get(block, "hash");
    std::vector<std::vector<BtcId>> inputIdsOfBlock;

    try {
        const auto& txs = block["tx"];

        for (const auto& tx : txs) {
            const auto& inputIds = generateTxInputs(dayDir, excludeAddresses, tx);

            if (inputIds.size() > 0) {
                inputIdsOfBlock.push_back(inputIds);
            }
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}:{}", dayDir, blockHash));
        logger.error(e.what());
    }

    return inputIdsOfBlock;
}

inline std::vector<BtcId> generateTxInputs(
    const std::string& dayDir,
    const std::set<BtcId>& excludeAddresses,
    const json& tx
) {
    std::string txHash = utils::json::get(tx, "hash");
    std::vector<BtcId> inputIds;

    try {
        auto& outputs = utils::json::get(tx, "out");
        for (auto& output : outputs) {
            auto addrItem = output.find("addr");
            if (addrItem == output.cend()) {
                continue;
            }
            BtcId addressId = addrItem.value();
            // 如果输出中包含需要排除的交易所地址，整笔交易都不考虑
            if (excludeAddresses.contains(addressId)) {
                logger.info(fmt::format("Exclude tx {}, address {} found in out", txHash, addressId));

                return std::vector<BtcId>();
            }
        }

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
                // 如果输入中包含需要排除的交易所地址，整笔交易都不考虑
                if (excludeAddresses.contains(addressId)) {
                    logger.info(fmt::format("Exclude tx {}, address {} found in input", txHash, addressId));

                    return std::vector<BtcId>();
                }

                inputIds.push_back(addressId);
            }
        }
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process tx {}:{}", dayDir, txHash));
        logger.error(e.what());
    }

    if (inputIds.size() > 0) {
        std::sort(inputIds.begin(), inputIds.end());
        auto newEnd = std::unique(inputIds.begin(), inputIds.end());
        if (newEnd != inputIds.end()) {
            inputIds.erase(newEnd, inputIds.end());
        }
    }

    return inputIds;
}

void dumpDayInputs(
    const char* filePath,
    const std::vector<std::vector<std::vector<BtcId>>>& txInputsOfDay
) {
    logger.info(fmt::format("Dump day_ins: {}", filePath));

    json txInputsOfDayJson(txInputsOfDay);
    std::ofstream txInputsOfDayFile(filePath);
    txInputsOfDayFile << txInputsOfDayJson;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}
