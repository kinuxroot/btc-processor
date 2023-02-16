#include "btc-config.h"
#include "btc_convert_exchanges/logger.h"

#include "logging/Logger.h"
#include "logging/handlers/FileHandler.h"
#include "utils/io_utils.h"
#include "utils/task_utils.h"
#include "utils/json_utils.h"
#include "utils/btc_utils.h"
#include "utils/mem_utils.h"
#include "utils/union_find.h"
#include "fmt/format.h"
#include <nlohmann/json.hpp>

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

std::vector<BtcId> convertAddress2Ids(
    const std::vector<std::string> addresses,
    std::vector<BtcId>& addressIds,
    std::map<std::string, BtcId>& address2Id,
    const std::string& appendFilePath
);
inline void logUsedMemory();

auto& logger = getLogger();

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_convert_exchanges <id2addr> <ex_addrs> <ex_addr_ids> <uf>\n" << std::endl;

        return EXIT_FAILURE;
    }

    const char* id2AddressFilePath = argv[1];
    logger.info("Load address2Id...");
    auto address2Id = utils::btc::loadAddress2Id(id2AddressFilePath);
    logger.info(fmt::format("Loaded address2Id: {} items", address2Id.size()));

    const char* exchangeAddressesFilePath = argv[2];
    std::vector<std::string> exchangeAddresses = utils::readLines(exchangeAddressesFilePath);

    logUsedMemory();

    std::vector<BtcId> addressIds;
    logger.info("Convert exchange addresses to ids");
    convertAddress2Ids(exchangeAddresses, addressIds, address2Id, id2AddressFilePath);
    logger.info(fmt::format("Converted {} address; total address count: {}", addressIds.size(), address2Id.size()));

    const char* exchangeAddressIdsFilePath = argv[3];
    logger.info(fmt::format("Dump exchange address ids to {}", exchangeAddressIdsFilePath));
    utils::writeLines(exchangeAddressIdsFilePath, addressIds);

    if (argc == 5) {
        const char* ufFilePath = argv[4];
        logger.info(fmt::format("Expand union find file from {}", ufFilePath));

        utils::btc::WeightedQuickUnion uf(1);
        uf.load(ufFilePath);

        auto originalSize = uf.getSize();
        uf.resize(address2Id.size());
        auto expandedSize = uf.getSize();

        logger.info(fmt::format(
            "Expand union find from {} to {}, expanded {}", 
            originalSize, expandedSize, expandedSize - originalSize
        ));

        uf.save(ufFilePath);
    }
    
    return EXIT_SUCCESS;
}

std::vector<BtcId> convertAddress2Ids(
    const std::vector<std::string> addresses,
    std::vector<BtcId>& addressIds,
    std::map<std::string, BtcId>& address2Id,
    const std::string& appendFilePath
) {
    std::ofstream addressIdFile(appendFilePath.c_str(), std::ios::app);

    BtcId maxId = address2Id.size();
    for (const auto& address : addresses) {
        auto addressIt = address2Id.find(address);
        if (addressIt == address2Id.end()) {
            addressIdFile << address << std::endl;

            address2Id[address] = maxId;
            addressIds.push_back(maxId);

            logger.info(fmt::format("Append new address: {}/{}", address, maxId));

            maxId = address2Id.size();
        }
        else {
            addressIds.push_back(addressIt->second);
            logger.info(fmt::format("Found address: {}/{}", address, maxId));
        }
    }

    return addressIds;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}