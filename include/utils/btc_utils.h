#pragma once

#include <string>
#include <vector>
#include <set>
#include <map>
#include <cstddef>

namespace utils::btc {
    std::vector<std::string> loadId2Address(const char* filePath);
    void dumpId2Address(const char* filePath, const std::set<std::string>& id2Address);
    std::map<std::string, std::size_t> generateAddress2Id(const std::vector<std::string>& id2Address);
}