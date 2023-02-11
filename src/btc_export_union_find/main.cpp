#include "btc-config.h"
#include "btc_gen_day_ins/logger.h"

#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "fmt/format.h"

#include <cstdlib>
#include <iostream>

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    using utils::btc::BtcSize;

    if (argc < 3) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_export_union_find <uf> <output_file>" << std::endl;

        return EXIT_FAILURE;
    }

    try {
        const char* unionFindFilePath = argv[1];
        logger.info(fmt::format("Load union find form {}", unionFindFilePath));

        utils::btc::WeightedQuickUnion quickUnion(1);
        quickUnion.load(unionFindFilePath);

        logger.info(fmt::format("Loaded ids: {}", quickUnion.getSize()));
        logger.info(fmt::format("Loaded cluster count: {}", quickUnion.getClusterCount()));

        const char* outputFilePath = argv[2];
        std::ofstream outputFile(outputFilePath);
        logger.info(fmt::format("Dump union find result to: {}", outputFilePath));
        
        const auto& quickUnionClusters = utils::btc::WeightedQuickUnionClusters(quickUnion);
        BtcSize dumpedClusterCount = 0;
        quickUnionClusters.forEach([&outputFile, &dumpedClusterCount](BtcId btcId, BtcSize btcSize) {
            outputFile << btcId << "," << btcSize << std::endl;

            ++dumpedClusterCount;
        });

        logger.info(fmt::format("Dump {} union find clusters", dumpedClusterCount));
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        logger.error(e.what());
    }

    return EXIT_SUCCESS;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}