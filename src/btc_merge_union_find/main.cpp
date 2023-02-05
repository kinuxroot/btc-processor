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
    if (argc < 3) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_merge_union_find <merged_ufs> <ufs>n" << std::endl;

        return EXIT_FAILURE;
    }

    try {
        int32_t inputFileCount = argc - 2;
        logger.info(fmt::format("UF file count: {}", inputFileCount));

        utils::btc::WeightedQuickUnion mergedQuickUnion(1);
        for (int32_t inputFileIndex = 0; inputFileIndex != inputFileCount; ++inputFileIndex) {
            const char* unionFindFilePath = argv[inputFileIndex + 2];
            logger.info(fmt::format("Merge union find form {}", unionFindFilePath));

            utils::btc::WeightedQuickUnion quickUnion(1);
            quickUnion.load(unionFindFilePath);

            if (inputFileIndex == 0) {
                mergedQuickUnion = quickUnion;
            }
            else {
                mergedQuickUnion.merge(quickUnion);
            }
        }

        const char* mergedFilePath = argv[1];
        logger.info(fmt::format("Dump merged file to: {}", mergedFilePath));
        mergedQuickUnion.save(mergedFilePath);
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