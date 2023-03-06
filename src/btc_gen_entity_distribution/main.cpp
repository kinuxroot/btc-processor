#include "btc-config.h"
#include "btc_gen_entity_distribution/logger.h"

#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/numeric_utils.h"
#include "fmt/format.h"
#include <argparse/argparse.hpp>

#include <cstdlib>
#include <iostream>
#include <thread>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <tuple>
#include <algorithm>

namespace fs = std::filesystem;
using BalanceValue = double;
using BalanceList = std::vector<BalanceValue>;

inline void logUsedMemory();

auto& logger = getLogger();

static argparse::ArgumentParser createArgumentParser();
std::vector<std::string> getAddressBalancePaths(
    const std::string& dirPath,
    uint32_t startYear,
    uint32_t endYear
);
std::size_t loadBalanceList(
    const std::string& inputFilePath,
    BalanceList& balanceList
);
void processEntityBalanceOfYears(
    uint32_t workerIndex,
    const std::vector<std::string>* addressBalanceFilePaths,
    const std::string& outputBaseDir,
    std::size_t initialBufferSize
);
void processYearEntityBalance(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceFilePathPrefix,
    std::size_t initialBufferSize
);
void generateEntityBalanceRanks(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceRanksFilePath,
    const BalanceList& balanceList
);
std::tuple<BalanceValue, BalanceValue> generateEntityBalanceBasicStatistics(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceBasicStatisticsFilePath,
    const BalanceList& balanceList
);
void generateEntityBalanceDistribution(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceRanksFilePath,
    const BalanceList& balanceList,
    BalanceValue minBalance,
    BalanceValue maxBalance
);

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

    try {
        std::string balanceBaseDirPath = argumentParser.get("balance_base_dir");

        auto startYear = argumentParser.get<uint32_t>("--start_year");
        logger.info(fmt::format("Using start year: {}", startYear));

        auto endYear = argumentParser.get<uint32_t>("--end_year");
        logger.info(fmt::format("Using end year: {}", endYear));

        auto initialBufferSize = argumentParser.get<std::size_t>("--buffer_size");
        logger.info(fmt::format("Using initial balance buffer size: {}", initialBufferSize));

        uint32_t workerCount = std::min(argumentParser.get<uint32_t>("--worker_count"), std::thread::hardware_concurrency());
        logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
        logger.info(fmt::format("Worker count: {}", workerCount));

        const std::vector<std::string>& addressBalanceFiles = getAddressBalancePaths(
            balanceBaseDirPath, startYear, endYear
        );
        const std::vector<std::vector<std::string>> taskChunks = utils::generateTaskChunks(addressBalanceFiles, workerCount);

        std::string outputBaseDirPath = argumentParser.get("output_base_dir");

        uint32_t workerIndex = 0;
        std::vector<std::future<void>> tasks;
        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(
                std::async(
                    processEntityBalanceOfYears,
                    workerIndex,
                    &taskChunk,
                    outputBaseDirPath,
                    initialBufferSize
                )
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

static argparse::ArgumentParser createArgumentParser() {
    argparse::ArgumentParser program("btc_filter_address_balance");

    program.add_argument("balance_base_dir")
        .required()
        .help("The base directory of entity balance list files");

    program.add_argument("output_base_dir")
        .required()
        .help("The base directory of output statistics");

    program.add_argument("--start_year")
        .help("Start year")
        .scan<'d', uint32_t>()
        .default_value(0u);

    program.add_argument("--end_year")
        .help("Start year")
        .scan<'d', uint32_t>()
        .default_value(0u);

    program.add_argument("--buffer_size")
        .help("Balance buffer initial size")
        .scan<'d', std::size_t>()
        .default_value(4096u);

    program.add_argument("-w", "--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    return program;
}

std::vector<std::string> getAddressBalancePaths(
    const std::string& dirPath,
    uint32_t startYear,
    uint32_t endYear
) {
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
        if (fileExtension != ".out") {
            continue;
        }

        const auto baseFilename = filePath.stem();
        uint32_t currentYear = std::stoi(baseFilename);
        if ((startYear && currentYear < startYear) || (endYear && currentYear > endYear)) {
            continue;
        }

        addressBalanceFilePaths.push_back(filePath.string());
        logger.info(fmt::format("Add balance file to task: {}", filePath.string()));
    }

    return addressBalanceFilePaths;
}

void processEntityBalanceOfYears(
    uint32_t workerIndex,
    const std::vector<std::string>* addressBalanceFilePaths,
    const std::string& outputBaseDir,
    std::size_t initialBufferSize
) {
    fs::path outputBaseDirPath(outputBaseDir);

    for (const std::string& addressBalanceFilePath : *addressBalanceFilePaths) {
        auto entityBalanceFilePathPrefix = outputBaseDirPath / fs::path(addressBalanceFilePath).filename();

        processYearEntityBalance(
            addressBalanceFilePath,
            entityBalanceFilePathPrefix.string(),
            initialBufferSize
        );
    }
}

void processYearEntityBalance(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceFilePathPrefix,
    std::size_t initialBufferSize
) {
    using utils::btc::BtcSize;

    BalanceList balanceList;
    balanceList.reserve(initialBufferSize);
    loadBalanceList(addressBalanceFilePath, balanceList);

    generateEntityBalanceRanks(addressBalanceFilePath, entityBalanceFilePathPrefix + ".ranks", balanceList);
    auto basicStatistics = generateEntityBalanceBasicStatistics(
        addressBalanceFilePath,
        entityBalanceFilePathPrefix + ".bs", balanceList
    );
    generateEntityBalanceDistribution(
        addressBalanceFilePath,
        entityBalanceFilePathPrefix + ".dist",
        balanceList,
        std::get<0>(basicStatistics),
        std::get<1>(basicStatistics)
    );
}

void generateEntityBalanceRanks(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceRanksFilePath,
    const BalanceList& balanceList
) {
    logger.info("Generate entity balance ranks");

    using utils::btc::BtcSize;

    static const std::vector<std::vector<uint32_t>> StaticEntityBalanceRankRanges{
        utils::range<uint32_t>(0, 100, 1),
        utils::range<uint32_t>(100, 1000, 100),
        utils::range<uint32_t>(1000, 10000, 1000),
        utils::range<uint32_t>(10000, 100000, 10000),
        utils::range<uint32_t>(100000, 1000000, 100000),
    };

    // 最后的范围必定包括最后一个实体余额
    std::vector<uint32_t> lastRankRange = utils::range<uint32_t>(1000000, balanceList.size(), 1000000);

    if (!lastRankRange.size() || lastRankRange.back() != balanceList.size()) {
        lastRankRange.push_back(balanceList.size());
    }

    std::vector<std::vector<uint32_t>> entityBalanceRankRanges = StaticEntityBalanceRankRanges;
    entityBalanceRankRanges.push_back(lastRankRange);

    BtcSize dumpedEntityCount = 0;
    std::ofstream outputFile(entityBalanceRanksFilePath.c_str());
    logger.info(fmt::format("Output ranks samples of entities to {}", entityBalanceRanksFilePath));

    for (const auto& entityBalanceRankRange : entityBalanceRankRanges) {
        for (uint32_t rankIndex : entityBalanceRankRange) {
            if (rankIndex < balanceList.size()) {
                outputFile << rankIndex << "," << std::setprecision(19) << balanceList[rankIndex] << std::endl;
                ++ dumpedEntityCount;
            }
        }
    }

    logger.info(fmt::format("dumpedEntityCount: {}", dumpedEntityCount));
}

std::tuple<BalanceValue, BalanceValue> generateEntityBalanceBasicStatistics(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceBasicStatisticsFilePath,
    const BalanceList& balanceList
) {
    logger.info("Generate entity balance basic statistics");

    BalanceValue maxBalance = std::numeric_limits<BalanceValue>::min();
    BalanceValue minBalance = std::numeric_limits<BalanceValue>::max();

    for (BalanceValue balance : balanceList) {
        maxBalance = std::max(balance, maxBalance);
        minBalance = std::min(balance, minBalance);
    }

    logger.info(fmt::format("Min: {:.19g}, Max: {:.19g}", minBalance, maxBalance));

    logger.info(fmt::format("Output basic statistics to {}", entityBalanceBasicStatisticsFilePath));
    std::ofstream outputFile(entityBalanceBasicStatisticsFilePath.c_str());
    outputFile << fmt::format("{:.19g},{:.19g}", minBalance, maxBalance) << std::endl;

    return std::make_tuple(minBalance, maxBalance);
}

void generateEntityBalanceDistribution(
    const std::string& addressBalanceFilePath,
    const std::string& entityBalanceDistributionFilePath,
    const BalanceList& balanceList,
    BalanceValue minBalance,
    BalanceValue maxBalance
) {
    logger.info("Generate entity balance balance distribution");

    using utils::btc::BtcSize;

    BalanceValue step = (maxBalance - minBalance) / 100;
    auto balanceRange = utils::range(minBalance, maxBalance, step);
    if (balanceRange.back() < maxBalance) {
        balanceRange.push_back(balanceRange.back() + step);
    }
    std::vector<BtcSize> entityBalanceDistribution(balanceRange.size(), 0);

    for (BalanceValue balance : balanceList) {
        auto rangeIndex = utils::binaryFindRangeLow(balanceRange, balance, 0, balanceRange.size());
        if (rangeIndex >= balanceRange.size()) {
            std::cerr << fmt::format("Balance out of range {}/{}", rangeIndex, balanceRange.size()) << std::endl;
            throw std::out_of_range(fmt::format("Balance out of range {}/{}", rangeIndex, balanceRange.size()));
        }

        ++ entityBalanceDistribution[rangeIndex];
    }

    logger.info(fmt::format("Output basic statistics to {}", entityBalanceDistributionFilePath));
    std::ofstream outputFile(entityBalanceDistributionFilePath.c_str());

    for (std::size_t rangeIndex = 0; rangeIndex != balanceRange.size(); ++rangeIndex) {
        std::string formattedLine = fmt::format(
            "{:.19g},{:.19g},{}",
            balanceRange[rangeIndex],
            balanceRange[rangeIndex] + step,
            entityBalanceDistribution[rangeIndex]
        );
        std::cout << formattedLine << std::endl;
        outputFile << formattedLine << std::endl;
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

        auto seperatorPos = line.rfind(',');
        if (seperatorPos == std::string::npos) {
            continue;
        }

        std::string balanceString = line.substr(seperatorPos + 1);
        BalanceValue btcValue = std::stod(balanceString);

        balanceList.push_back(btcValue);
        ++loadedCount;
    }

    return loadedCount;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}