#pragma once

#include <string>
#include <vector>
#include <set>
#include <map>
#include <cstddef>
#include <cstdint>

using BtcId = uint32_t;

namespace utils::btc {
    std::vector<std::string> loadId2Address(const char* filePath);
    void loadId2Address(const char* filePath, std::vector<std::string>& id2Address);
    void dumpId2Address(const char* filePath, const std::set<std::string>& id2Address);
    std::map<std::string, BtcId> generateAddress2Id(const std::vector<std::string>& id2Address);
    std::map<std::string, BtcId> loadAddress2Id(const char* filePath);
    void loadDayInputs(const char* filePath, std::vector<std::vector<std::vector<BtcId>>>& blocks);
}