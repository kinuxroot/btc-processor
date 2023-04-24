#include "btc-config.h"
#include "btc_gen_block_statistics/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/mem_utils.h"
#include "fmt/format.h"
#include <nlohmann/json.hpp>
#include <argparse/argparse.hpp>

#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <thread>
#include <map>
#include <limits>
#include <iomanip>
#include <algorithm>

using json = nlohmann::json;

const auto INVALID_ENTITY_YEAR = std::numeric_limits<int16_t>::min();

namespace fs = std::filesystem;

std::vector<
    std::pair<std::string, std::vector<std::string>>
> groupDaysListByYears(const std::vector<std::string>& daysList, uint32_t startYear, uint32_t endYear);

static argparse::ArgumentParser createArgumentParser();

uint64_t calculateBlockStatisticsOfDays(const std::string& dayDir);
uint64_t calculateBlockStatisticsOfBlock(const json& block);
void dumpYearBlockCounts(
    const std::string& outputFilePath,
    const std::vector<std::pair<std::string, uint64_t>>& yearBlockCounts
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

    std::vector<std::string> daysList = utils::readLines(daysListFilePath);
    logger.info(fmt::format("Read tasks count: {}", daysList.size()));

    std::string outputBaseDirPath = argumentParser.get("output_base_dir");
    fs::create_directories(outputBaseDirPath);

    fs::path summaryOutputBaseDirPath = fs::path(outputBaseDirPath) / "summary";
    fs::create_directories(summaryOutputBaseDirPath);

    uint32_t workerCount = std::min(
        argumentParser.get<uint32_t>("--worker_count"),
        std::thread::hardware_concurrency()
    );
    logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
    logger.info(fmt::format("Worker count: {}", workerCount));

    auto startYear = argumentParser.get<uint32_t>("--start_year");
    logger.info(fmt::format("Using start year: {}", startYear));

    auto endYear = argumentParser.get<uint32_t>("--end_year");
    logger.info(fmt::format("Using end year: {}", endYear));

    auto groupedDaysList = groupDaysListByYears(daysList, startYear, endYear);
    std::vector<std::pair<std::string, uint64_t>> yearBlockCountList;
    for (const auto& yearDaysList : groupedDaysList) {
        const auto& year = yearDaysList.first;
        const auto& lastDateDir = yearDaysList.second.back();
        uint64_t lastBlockIndex = calculateBlockStatisticsOfDays(lastDateDir);
        yearBlockCountList.push_back(std::make_pair(year, lastBlockIndex));
    }

    fs::path yearOutputFilePath = fs::path(summaryOutputBaseDirPath) / "block_counts.list";
    dumpYearBlockCounts(yearOutputFilePath.string(), yearBlockCountList);

    return EXIT_SUCCESS;
}

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_gen_address_statistics");

    program.add_argument("days_dir_list")
        .required()
        .help("The list of days dirs");

    program.add_argument("output_base_dir")
        .required()
        .help("The base directory of statistics files");

    program.add_argument("--start_year")
        .help("Start year")
        .scan<'d', uint32_t>()
        .default_value(0u);

    program.add_argument("--end_year")
        .help("End year")
        .scan<'d', uint32_t>()
        .default_value(0u);

    program.add_argument("-w", "--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    return program;
}

std::vector<std::pair<std::string, std::vector<std::string>>>
groupDaysListByYears(const std::vector<std::string>& daysList, uint32_t startYear, uint32_t endYear) {
    std::map<std::string, std::vector<std::string>> yearDaysListMap;
    
    for (const auto& dayDirPathLine : daysList) {
        fs::path dayDirPath(dayDirPathLine);

        const std::string& dirName = dayDirPath.filename().string();
        const auto& year = dirName.substr(0, 4);
        const auto yearValue = std::stoi(year);
        if ((startYear > 0 && yearValue < startYear) || (endYear > 0 && yearValue > endYear)) {
            continue;
        }

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

    for (auto& yearDays : groupedDaysList) {
        logger.info(fmt::format("Last year before sort: {} {}", yearDays.first, yearDays.second.back()));
        std::sort(yearDays.second.begin(), yearDays.second.end());
        logger.info(fmt::format("Last year after sort: {} {}", yearDays.first, yearDays.second.back()));
    }

    return groupedDaysList;
}

uint64_t calculateBlockStatisticsOfDays(
    const std::string& dayDir
) {
    try {
        auto convertedBlocksFilePath = fmt::format("{}/{}", dayDir, "converted-block-list.json");
        logger.info(fmt::format("Process combined blocks file: {}", dayDir));

        std::ifstream convertedBlocksFile(convertedBlocksFilePath.c_str());
        if (!convertedBlocksFile.is_open()) {
            logger.warning(fmt::format("Skip processing blocks by date because file not exists: {}", convertedBlocksFilePath));
            return 0;
        }

        logUsedMemory();
        json blocks;
        convertedBlocksFile >> blocks;
        logger.info(fmt::format("Block count: {} {}", dayDir, blocks.size()));
        logUsedMemory();

        const auto& lastBlock = blocks.back();
        auto lastBlockIndex = calculateBlockStatisticsOfBlock(lastBlock);

        logUsedMemory();

        logger.info(fmt::format("Finished process blocks by date: {}", dayDir));

        logUsedMemory();

        return lastBlockIndex;
    }
    catch (const std::exception& e) {
        logger.error(fmt::format("Error when process blocks by date: {}", dayDir));
        logger.error(e.what());

        return 0;
    }
}

uint64_t calculateBlockStatisticsOfBlock(
    const json& block
) {
    std::string blockHash = utils::json::get(block, "hash");

    try {
        uint64_t blockIndex = block.at("block_index");

        return blockIndex;
    }
    catch (std::exception& e) {
        logger.error(fmt::format("Error when process block {}", blockHash));
        logger.error(e.what());

        return 0;
    }
}

void dumpYearBlockCounts(
    const std::string& outputFilePath,
    const std::vector<std::pair<std::string, uint64_t>>& yearBlockCounts
) {
    logger.info(fmt::format("Dump block count of years to: {}", outputFilePath));

    std::ofstream outputFile(outputFilePath.c_str());
    for (const auto& yearBlockCount : yearBlockCounts) {
        outputFile << fmt::format("{}: {}", yearBlockCount.first, yearBlockCount.second) << std::endl;
    }
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}