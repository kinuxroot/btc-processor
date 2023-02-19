#include "btc-config.h"
#include "btc_uf_exchanges/logger.h"

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
    if (argc < 4 || argc == 5) {
        std::cerr << "Invalid arguments!\n\nUsage: btc_uf_exchanges <uf> <ex_ids> <ex_uf_ids> <id2addr> <ex_uf_addr>\n" << std::endl;

        return EXIT_FAILURE;
    }
    
    const char* ufFilePath = argv[1];
    logger.info(fmt::format("Load union find file from {}", ufFilePath));
    utils::btc::WeightedQuickUnion uf(1);
    uf.load(ufFilePath);

    logUsedMemory();

    logger.info("Loading exchange address ids");
    const char* exchangeAddressIdsFilePath = argv[2];
    std::vector<BtcId> addressIds = utils::readLines<BtcId>(
        exchangeAddressIdsFilePath,
        [](const std::string& line) -> BtcId {
            return std::stoi(line);
        }
    );

    logUsedMemory();

    logger.info("Generating exchange root address ids");

    std::set<BtcId> exchangeRootAddresseIds;
    for (BtcId addressId : addressIds) {
        exchangeRootAddresseIds.insert(uf.findRoot(addressId));
    }

    logger.info("Generating all address ids by union find");

    std::vector<BtcId> unionFoundExchangedAddressIds;
    auto maxId = uf.getSize();
    for (BtcId currentId = 0; currentId < maxId; ++currentId) {
        BtcId currentRoot = uf.findRoot(currentId);
        if (exchangeRootAddresseIds.contains(currentRoot)) {
            unionFoundExchangedAddressIds.push_back(currentId);
        }
    }

    const char* unionFoundExchangedAddressIdsFilePath = argv[3];
    logger.info(fmt::format("Writing {} address ids by union find", unionFoundExchangedAddressIds.size()));
    utils::writeLines(unionFoundExchangedAddressIdsFilePath, unionFoundExchangedAddressIds);

    if (argc > 5) {
        const char* address2IdFilePath = argv[4];
        logger.info("Load id2Address...");
        auto id2Address = utils::btc::loadId2Address(address2IdFilePath);
        logger.info(fmt::format("Loaded id2Address: {} items", id2Address.size()));

        const char* unionFoundExchangedAddressesFilePath = argv[5];
        std::vector<std::string> unionFoundExchangedAddresses;
        for (BtcId addressId : unionFoundExchangedAddressIds) {
            const std::string exchangeAddress = id2Address[addressId];
            unionFoundExchangedAddresses.push_back(exchangeAddress);
        }

        utils::writeLines(unionFoundExchangedAddressesFilePath, unionFoundExchangedAddresses);
        logger.info(fmt::format("Writing {} addresses by union find", unionFoundExchangedAddresses.size()));
    }
    
    return EXIT_SUCCESS;
}

inline void logUsedMemory() {
    auto usedMemory = utils::mem::getAllocatedMemory();
    logger.debug(fmt::format("Used memory: {}GB {}MB", usedMemory / 1024 / 1024, usedMemory / 1024));
}