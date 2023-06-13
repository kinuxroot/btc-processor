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
using EntityYearList = std::vector<int16_t>;
using SortedBalanceItem = std::pair<std::size_t, BalanceValue>;
using SortedBalanceList = std::vector<SortedBalanceItem>;
using CountList = std::vector<uint8_t>;

struct AverageFilterOptions {
    double removeLowValue = 0.0;
    bool removeMiner = false;
    bool removeLabeledExchange = false;
    bool removeFoundExchange = false;
    double removeTopPercents = 0.0;
    bool onlyLongTerm = false;
    bool exportHighest = false;
};

int compareBalanceItem(const void* x, const void* y) {
    const SortedBalanceItem lhs = *static_cast<const SortedBalanceItem*>(x);
    const SortedBalanceItem rhs = *static_cast<const SortedBalanceItem*>(y);

    if (rhs.second > lhs.second) {
        return 1;
    }
    else if (rhs.second < lhs.second) {
        return -1;
    }

    return 0;
}

const auto INVALID_ENTITY_YEAR = std::numeric_limits<int16_t>::min();

inline void logUsedMemory();

auto& logger = getLogger();

static argparse::ArgumentParser createArgumentParser();
std::vector<std::pair<uint32_t, std::string>> getEntityBalanceYearItems(
    const std::string& dirPath,
    uint32_t startYear,
    uint32_t endYear
);
void processEntityBalanceOfYears(
    uint32_t workerIndex,
    const std::vector<std::pair<uint32_t, std::string>>* entityBalanceYearItems,
    const std::string& addressReportBaseDir,
    const std::string& outputBaseDir,
    const EntityYearList* entityYearList,
    const std::vector<utils::btc::ClusterLabels>* clusterLabels,
    std::size_t initialBufferSize,
    std::uint32_t distributionSegment,
    const AverageFilterOptions* averageFilterOptions
);
void processYearEntityBalance(
    uint32_t year,
    const std::string& entityBalanceFilePath,
    const std::string& entityCountListFilePath,
    const std::string& entityBalanceFilePathPrefix,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    std::size_t initialBufferSize,
    std::uint32_t distributionSegment,
    const AverageFilterOptions& averageFilterOptions
);
void generateEntityAverageBalance(
    const std::string& entityAverageFilePath,
    const SortedBalanceList& balanceList,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels
);
void generateRichestEntites(
    const std::string& richestEntitiesFilePath,
    const EntityYearList& entityYearList,
    const SortedBalanceList& balanceList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels
);
void generateEntityBalanceRanks(
    const std::string& entityBalanceRanksFilePath,
    const SortedBalanceList& balanceList
);
std::tuple<BalanceValue, BalanceValue> generateEntityBalanceBasicStatistics(
    const std::string& entityBalanceBasicStatisticsFilePath,
    const SortedBalanceList& balanceList,
    std::size_t zeroEntityCount
);
void generateEntityBalanceDistribution(
    const std::string& entityBalanceRanksFilePath,
    const SortedBalanceList& balanceList,
    BalanceValue minBalance,
    BalanceValue maxBalance,
    std::uint32_t distributionSegment
);

std::size_t loadBalanceList(
    const std::string& inputFilePath,
    BalanceList& balanceList,
    std::size_t& zeroEntityCount
);
SortedBalanceList sortBalanceList(
    uint32_t year,
    const BalanceList& balanceList,
    const CountList& entityCountList,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    size_t initialEntityCount,
    const AverageFilterOptions& averageFilterOptions
);
std::size_t loadCountList(
    const std::string& inputFilePath,
    CountList& countList
);
std::size_t loadYearList(
    const std::string& inputFilePath,
    EntityYearList& yearList
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

        auto distributionSegment = argumentParser.get<uint32_t>("--distribution_segments");
        logger.info(fmt::format("Using end distribution segment: {}", distributionSegment));

        uint32_t workerCount = std::min(argumentParser.get<uint32_t>("--worker_count"), std::thread::hardware_concurrency());
        logger.info(fmt::format("Hardware Concurrency: {}", std::thread::hardware_concurrency()));
        logger.info(fmt::format("Worker count: {}", workerCount));

        const auto& entityBalanceYearItems = getEntityBalanceYearItems(
            balanceBaseDirPath, startYear, endYear
        );
        const auto taskChunks = utils::generateTaskChunks(
            entityBalanceYearItems,
            workerCount
        );

        std::string addressReportDirPath = argumentParser.get("address_report_dir");
        fs::path entityYearFilePath = fs::path(addressReportDirPath) / "entity-year.out";
        EntityYearList entityYearList;
        std::size_t loadedEntityYears = loadYearList(entityYearFilePath.string(), entityYearList);
        logger.info(fmt::format("Loaded entity years: {}", loadedEntityYears));

        auto addressCount = argumentParser.get<uint32_t>("--address_count");
        logger.info(fmt::format("Using address count: {}", addressCount));

        const std::string entityLabelFilePath = argumentParser.get("--entity_label_file");
        std::ifstream entityLabelFile(entityLabelFilePath, std::ios::binary);
        std::vector<utils::btc::ClusterLabels> clusterLabels(addressCount);
        logger.info("Load clusterLabels...");
        entityLabelFile.read(
            reinterpret_cast<char*>(clusterLabels.data()),
            clusterLabels.size() * sizeof(utils::btc::ClusterLabels)
        );
        logger.info(fmt::format("Loaded clusterLabels {} items", clusterLabels.size()));

        std::string outputBaseDirPath = argumentParser.get("output_base_dir");

        auto removeLowValue = argumentParser.get<double>("--remove_low_value");
        logger.info(fmt::format("Using remove low value: {}", removeLowValue));

        auto removeMiner = argumentParser.get<bool>("--remove_miner");
        logger.info(fmt::format("Using miner: {}", removeMiner));

        auto removeLabeledExchange = argumentParser.get<bool>("--remove_labeled_exchange");
        logger.info(fmt::format("Using labeled exchange: {}", removeLabeledExchange));

        auto removeFoundExchange = argumentParser.get<bool>("--remove_found_exchange");
        logger.info(fmt::format("Using found exchange: {}", removeFoundExchange));

        auto removeTopPercents = argumentParser.get<double>("--remove_top_percents");
        logger.info(fmt::format("Using remove top percents: {}", removeTopPercents));

        auto onlyLongTerm = argumentParser.get<bool>("--only_long_term");
        logger.info(fmt::format("Using remove long term: {}", onlyLongTerm));

        auto exportHighest = argumentParser.get<bool>("--export_highest");
        logger.info(fmt::format("Using export highest: {}", exportHighest));

        AverageFilterOptions averageFilterOptions{
            .removeLowValue = removeLowValue,
            .removeMiner = removeMiner,
            .removeLabeledExchange = removeLabeledExchange,
            .removeFoundExchange = removeFoundExchange,
            .removeTopPercents = removeTopPercents,
            .onlyLongTerm = onlyLongTerm,
            .exportHighest = exportHighest,
        };

        uint32_t workerIndex = 0;
        std::vector<std::future<void>> tasks;
        for (const auto& taskChunk : taskChunks) {
            tasks.push_back(
                std::async(
                    processEntityBalanceOfYears,
                    workerIndex,
                    &taskChunk,
                    addressReportDirPath,
                    outputBaseDirPath,
                    &entityYearList,
                    &clusterLabels,
                    initialBufferSize,
                    distributionSegment,
                    &averageFilterOptions
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

    program.add_argument("address_report_dir")
        .required()
        .help("The base directory of address report files");

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

    program.add_argument("--entity_label_file")
        .help("Entity label file")
        .required();

    program.add_argument("--address_count")
        .help("Address id count")
        .scan<'d', uint32_t>()
        .required();

    program.add_argument("--buffer_size")
        .help("Balance buffer initial size")
        .scan<'d', std::size_t>()
        .default_value(4096u);

    program.add_argument("--distribution_segments")
        .help("Distribution segments")
        .scan<'d', std::uint32_t>()
        .default_value(100u);

    program.add_argument("-w", "--worker_count")
        .help("Max worker count")
        .scan<'d', uint32_t>()
        .required();

    program.add_argument("--remove_low_value")
        .help("Remove the entity lower than specified value")
        .scan<'g', double>()
        .default_value(0.0);

    program.add_argument("--remove_miner")
        .help("To remove miners")
        .default_value(false)
        .implicit_value(true);

    program.add_argument("--remove_labeled_exchange")
        .help("To remove labeled exchanges")
        .default_value(false)
        .implicit_value(true);

    program.add_argument("--remove_found_exchange")
        .help("To remove found exchanges")
        .default_value(false)
        .implicit_value(true);

    program.add_argument("--remove_top_percents")
        .help("Remove top entities after remove below items of percents")
        .scan<'g', double>()
        .default_value(0.0);

    program.add_argument("--only_long_term")
        .help("Only generate users of long term users")
        .default_value(false)
        .implicit_value(true);

    program.add_argument("--export_highest")
        .help("Export highest entities")
        .default_value(false)
        .implicit_value(true);

    return program;
}

std::vector<std::pair<uint32_t, std::string>> getEntityBalanceYearItems(
    const std::string& dirPath,
    uint32_t startYear,
    uint32_t endYear
) {
    std::vector<std::pair<uint32_t, std::string>> entityBalanceYearItems;

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

        entityBalanceYearItems.push_back(std::make_pair(currentYear, filePath.string()));
        logger.info(fmt::format("Add balance file to task: {}", filePath.string()));
    }

    return entityBalanceYearItems;
}

void processEntityBalanceOfYears(
    uint32_t workerIndex,
    const std::vector<std::pair<uint32_t, std::string>>* entityBalanceYearItems,
    const std::string& addressReportBaseDir,
    const std::string& outputBaseDir,
    const EntityYearList* entityYearList,
    const std::vector<utils::btc::ClusterLabels>* clusterLabels,
    std::size_t initialBufferSize,
    std::uint32_t distributionSegment,
    const AverageFilterOptions* averageFilterOptions
) {
    fs::path outputBaseDirPath(outputBaseDir);

    for (const auto& entityBalanceYearItem : *entityBalanceYearItems) {
        auto year = entityBalanceYearItem.first;
        auto entityCountListFilePath = fs::path(addressReportBaseDir) / "entity" / (std::to_string(year) + ".new");

        const auto& entityBalanceFilePath = entityBalanceYearItem.second;
        auto entityBalanceFilePathPrefix = outputBaseDirPath / fs::path(entityBalanceFilePath).filename();

        processYearEntityBalance(
            year,
            entityBalanceFilePath,
            entityCountListFilePath.string(),
            entityBalanceFilePathPrefix.string(),
            *entityYearList,
            *clusterLabels,
            initialBufferSize,
            distributionSegment,
            *averageFilterOptions
        );
    }
}

void processYearEntityBalance(
    uint32_t year,
    const std::string& entityBalanceFilePath,
    const std::string& entityCountListFilePath,
    const std::string& entityBalanceFilePathPrefix,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    std::size_t initialBufferSize,
    std::uint32_t distributionSegment,
    const AverageFilterOptions& averageFilterOptions
) {
    using utils::btc::BtcSize;

    BalanceList balanceList;
    balanceList.reserve(initialBufferSize);
    std::size_t zeroEntityCount = 0;
    loadBalanceList(entityBalanceFilePath, balanceList, zeroEntityCount);
    logUsedMemory();
    std::size_t nonzeroEntityCount = balanceList.size() - zeroEntityCount;

    CountList entityCountList;
    loadCountList(entityCountListFilePath, entityCountList);
    logUsedMemory();

    const auto& sortedBalanceList = sortBalanceList(
        year,
        balanceList,
        entityCountList,
        entityYearList,
        clusterLabels,
        nonzeroEntityCount,
        averageFilterOptions
    );
    logUsedMemory();

    generateEntityAverageBalance(
        entityBalanceFilePathPrefix + ".avgs",
        sortedBalanceList,
        entityYearList,
        clusterLabels
    );

    if (averageFilterOptions.exportHighest) {
        generateRichestEntites(
            entityBalanceFilePathPrefix + ".richest",
            entityYearList,
            sortedBalanceList,
            clusterLabels
        );
    }
    //generateEntityBalanceRanks(entityBalanceFilePathPrefix + ".ranks", sortedBalanceList);
    //auto basicStatistics = generateEntityBalanceBasicStatistics(
    //    entityBalanceFilePathPrefix + ".bs",
    //    sortedBalanceList,
    //    zeroEntityCount
    //);
    //generateEntityBalanceDistribution(
    //    entityBalanceFilePathPrefix + ".dist",
    //    sortedBalanceList,
    //    std::get<0>(basicStatistics),
    //    std::get<1>(basicStatistics),
    //    distributionSegment
    //);
}

void generateEntityAverageBalance(
    const std::string& entityAverageFilePath,
    const SortedBalanceList& balanceList,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels
) {
    logger.info("Generate entity average balance");

    double totalBalance = 0;
    for (const auto& balanceItem : balanceList) {
        totalBalance += balanceItem.second;
    }

    double averageBalance = totalBalance / balanceList.size();
    std::ofstream outputFile(entityAverageFilePath.c_str());
    logger.info(fmt::format("Output all average balance {} to {}", averageBalance, entityAverageFilePath));
    outputFile << fmt::format("All average: {:.19g}", averageBalance) << std::endl;

    outputFile << "Average balance of segments" << std::endl;
    outputFile << "BeginPercent,EndPercent,BeginRank,EndRank,Balance" << std::endl;

    for (std::size_t startPercent = 0, startIndex = 0;
        startPercent != 100;
        ++startPercent
    ) {
        auto endPercent = startPercent + 1;
        auto endIndex = static_cast<std::size_t>(
            std::ceil(static_cast<double>(endPercent) * balanceList.size() / 100)
        );
        if (startPercent == 99) {
            endIndex = balanceList.size();
        }

        double segmentTotalBalance = 0;
        for (auto currentIndex = startIndex; currentIndex != endIndex; ++currentIndex) {
            segmentTotalBalance += balanceList[currentIndex].second;
        }
        double segmentEntityCount = endIndex - startIndex;
        double segmentAverageBalance = segmentTotalBalance / segmentEntityCount;

        outputFile << fmt::format(
            "{},{},{},{},{:.19g}",
            startPercent, startPercent + 1, startIndex, endIndex,
            segmentAverageBalance
        ) << std::endl;

        startIndex = endIndex;
    }
}

void generateRichestEntites(
    const std::string& richestEntitiesFilePath,
    const EntityYearList& entityYearList,
    const SortedBalanceList& balanceList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels
) {
    logger.info("Generate riches entities");

    std::ofstream outputFile(richestEntitiesFilePath.c_str());
    logger.info(fmt::format("Output richest entities balance to {}", richestEntitiesFilePath));
    outputFile <<
        "Rank,Entity,Balance,Year,isMiner,isLabeldExchange,isFoundExchange" <<
        std::endl;

    std::size_t maxEntityCount = std::min(balanceList.size(), static_cast<std::size_t>(10000));
    for (uint32_t entityRank = 0; entityRank != maxEntityCount; ++entityRank) {
        const auto& entityBalanceItem = balanceList[entityRank];
        auto entityId = entityBalanceItem.first;

        utils::btc::ClusterLabels clusterLabel = clusterLabels[entityId];
        bool isMiner = clusterLabel.isMiner;
        bool isLabeldExchange = clusterLabel.isLabeldExchange;
        bool isFoundExchange = clusterLabel.isFoundExchange;

        outputFile << fmt::format(
            "{},{},{:.19g},{},{},{},{}",
            entityRank + 1, entityId, entityBalanceItem.second, entityYearList[entityId],
            isMiner, isLabeldExchange, isFoundExchange
        ) << std::endl;
    }
}

void generateEntityBalanceRanks(
    const std::string& entityBalanceRanksFilePath,
    const SortedBalanceList& balanceList
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

    std::vector<std::vector<uint32_t>> entityBalanceRankRanges = StaticEntityBalanceRankRanges;

    if (balanceList.size() > 1000000) {
        // 最后的范围必定包括最后一个实体余额
        std::vector<uint32_t> lastRankRange = utils::range<uint32_t>(1000000, balanceList.size(), 1000000);

        if (!lastRankRange.size() || lastRankRange.back() != balanceList.size()) {
            lastRankRange.push_back(balanceList.size());
        }
        entityBalanceRankRanges.push_back(lastRankRange);
    }

    BtcSize dumpedEntityCount = 0;
    std::ofstream outputFile(entityBalanceRanksFilePath.c_str());
    logger.info(fmt::format("Output ranks samples of entities to {}", entityBalanceRanksFilePath));

    for (const auto& entityBalanceRankRange : entityBalanceRankRanges) {
        for (uint32_t rankIndex : entityBalanceRankRange) {
            if (rankIndex < balanceList.size()) {
                outputFile << rankIndex << "," << std::setprecision(19) << balanceList[rankIndex].second << std::endl;
                ++ dumpedEntityCount;
            }
        }
    }

    logger.info(fmt::format("dumpedEntityCount: {}", dumpedEntityCount));
}

std::tuple<BalanceValue, BalanceValue> generateEntityBalanceBasicStatistics(
    const std::string& entityBalanceBasicStatisticsFilePath,
    const SortedBalanceList& balanceList,
    std::size_t zeroEntityCount
) {
    logger.info("Generate entity balance basic statistics");

    BalanceValue maxBalance = std::numeric_limits<BalanceValue>::min();
    BalanceValue minBalance = std::numeric_limits<BalanceValue>::max();

    for (const auto& balanceItem : balanceList) {
        maxBalance = std::max(balanceItem.second, maxBalance);
        minBalance = std::min(balanceItem.second, minBalance);
    }

    logger.info(fmt::format(
        "Min: {:.19g}, Max: {:.19g}, Zero entity count: {}",
        minBalance,
        maxBalance,
        zeroEntityCount
    ));

    logger.info(fmt::format("Output basic statistics to {}", entityBalanceBasicStatisticsFilePath));
    std::ofstream outputFile(entityBalanceBasicStatisticsFilePath.c_str());
    outputFile << fmt::format("{:.19g},{:.19g},{}", minBalance, maxBalance, zeroEntityCount) << std::endl;

    return std::make_tuple(minBalance, maxBalance);
}

void generateEntityBalanceDistribution(
    const std::string& entityBalanceDistributionFilePath,
    const SortedBalanceList& balanceList,
    BalanceValue minBalance,
    BalanceValue maxBalance,
    uint32_t distributionSegment
) {
    logger.info("Generate entity balance balance distribution");

    using utils::btc::BtcSize;

    BalanceValue step = (maxBalance - minBalance) / distributionSegment;
    auto balanceRange = utils::range(minBalance, maxBalance, step);
    if (balanceRange.back() < maxBalance) {
        balanceRange.push_back(balanceRange.back() + step);
    }
    std::vector<BtcSize> entityBalanceDistribution(balanceRange.size(), 0);

    for (const auto& balanceItem : balanceList) {
        auto rangeIndex = utils::binaryFindRangeLow(balanceRange, balanceItem.second, 0, balanceRange.size());
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
    BalanceList& balanceList,
    std::size_t& zeroEntityCount
) {
    logger.info(fmt::format("Load balance from: {}", inputFilePath));
    std::ifstream inputFile(inputFilePath.c_str(), std::ios::binary);

    std::size_t balanceSize = 0;
    inputFile.read(reinterpret_cast<char*>(&balanceSize), sizeof(balanceSize));
    balanceList.resize(balanceSize);
    inputFile.read(reinterpret_cast<char*>(balanceList.data()), sizeof(BalanceValue) * balanceSize);
    logger.info(fmt::format("Loaded {} balance from: {}", balanceSize, inputFilePath));

    zeroEntityCount = std::count_if(balanceList.begin(), balanceList.end(), [](BalanceValue value) {
        return value <= 0;
    });

    return balanceSize;
}

bool isToRemoveEntity(
    BalanceValue value,
    uint16_t currentYear,
    uint16_t entityYear,
    const utils::btc::ClusterLabels clusterLabel,
    const AverageFilterOptions& averageFilterOptions
) {
    bool onlyLongTerm = averageFilterOptions.onlyLongTerm;
    if (onlyLongTerm && entityYear >= currentYear) {
        return true;
    }

    // 默认为0，会移除所有余额为0的实体
    double removeLowValue = averageFilterOptions.removeLowValue;
    if (value <= removeLowValue) {
        return true;
    }

    bool removeMiner = averageFilterOptions.removeMiner;
    if (removeMiner && clusterLabel.isMiner) {
        return true;
    }
    bool removeLabeledExchange = averageFilterOptions.removeLabeledExchange;
    if (removeLabeledExchange && clusterLabel.isLabeldExchange) {
        return true;
    }
    bool removeFoundExchange = averageFilterOptions.removeFoundExchange;
    if (removeFoundExchange && clusterLabel.isFoundExchange) {
        return true;
    }

    return false;
}

SortedBalanceList sortBalanceList(
    uint32_t year,
    const BalanceList& balanceList,
    const CountList& entityCountList,
    const EntityYearList& entityYearList,
    const std::vector<utils::btc::ClusterLabels>& clusterLabels,
    std::size_t initialEntityCount,
    const AverageFilterOptions& averageFilterOptions
) {
    logger.info(fmt::format("Sort balance list with initial count: {}", initialEntityCount));

    SortedBalanceList sortedBalanceList;
    sortedBalanceList.reserve(initialEntityCount);
    logUsedMemory();

    size_t addressId = 0;
    for (auto countValue : entityCountList) {
        // 此处会删除余额为0的实体
        if (countValue && !isToRemoveEntity(
            balanceList[addressId],
            year,
            entityYearList[addressId],
            clusterLabels[addressId],
            averageFilterOptions)
        ) {
            sortedBalanceList.push_back(std::make_pair(addressId, balanceList[addressId]));
        }

        ++addressId;
    }
    logUsedMemory();

    logger.info(fmt::format("Begin to sort entities: {}", sortedBalanceList.size()));
    qsort(sortedBalanceList.data(), sortedBalanceList.size(), sizeof(SortedBalanceItem), compareBalanceItem);
    logger.info(fmt::format("Finished sort entities: {}", sortedBalanceList.size()));

    auto removeTopPercents = averageFilterOptions.removeTopPercents;
    if (removeTopPercents > 0) {
        size_t topEntityCount = static_cast<size_t>(
            sortedBalanceList.size() * averageFilterOptions.removeTopPercents / 100
        );
        logger.info(fmt::format("Remove top entities: {}% -> {}", removeTopPercents, topEntityCount));
        sortedBalanceList.erase(sortedBalanceList.begin(), sortedBalanceList.begin() + topEntityCount);
    }

    return sortedBalanceList;
}

std::size_t loadYearList(
    const std::string& inputFilePath,
    EntityYearList& yearList
) {
    logger.info(fmt::format("Load count from: {}", inputFilePath));
    std::ifstream inputFile(inputFilePath.c_str(), std::ios::binary);
    std::size_t loadedCount = 0;

    inputFile.read(reinterpret_cast<char*>(&loadedCount), sizeof(loadedCount));
    yearList.resize(loadedCount);
    inputFile.read(reinterpret_cast<char*>(yearList.data()), yearList.size() * sizeof(int16_t));

    return loadedCount;
}

std::size_t loadCountList(
    const std::string& inputFilePath,
    CountList& countList
) {
    logger.info(fmt::format("Load count from: {}", inputFilePath));
    std::ifstream inputFile(inputFilePath.c_str(), std::ios::binary);
    std::size_t loadedCount = 0;

    inputFile.read(reinterpret_cast<char*>(&loadedCount), sizeof(loadedCount));
    countList.resize(loadedCount);
    inputFile.read(reinterpret_cast<char*>(countList.data()), countList.size());
    logger.info(fmt::format("Loaded {} count from: {}", countList.size(), inputFilePath));

    return loadedCount;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}