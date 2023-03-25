#include "btc-config.h"
#include "btc_textify_union_find/logger.h"

#include "utils/io_utils.h"
#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "fmt/format.h"

#include <cstdlib>
#include <iostream>

static const size_t OUTPUT_BUFFER_COUNT = 1000000;
static const size_t OUTPUT_BUFFER_SIZE = OUTPUT_BUFFER_COUNT * 30;

inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    using utils::btc::BtcSize;

    if (argc < 3) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_textify_union_find <uf> <output_file>" << std::endl;

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

        BtcSize maxBtcId = quickUnion.getSize();
        std::string outputBuffer;
        outputBuffer.reserve(OUTPUT_BUFFER_SIZE);
        for (BtcSize btcId = 0; btcId != maxBtcId; ++btcId) {
            BtcId btcIdRoot = quickUnion.findRoot(btcId);
            outputBuffer.append(std::to_string(btcIdRoot)).append("\n");

            if (btcId % 1000000 == 0) {
                outputFile << outputBuffer;
                outputBuffer.resize(0);
                outputBuffer.reserve(OUTPUT_BUFFER_SIZE);

                logger.info(fmt::format("Dumpped btc id count: {}", btcId));
            }
        }

        outputFile << outputBuffer;
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