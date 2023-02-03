#pragma once

#include "fmt/format.h"

#include <string>
#include <vector>
#include <set>

namespace utils {
    std::string readFile(const std::string& filePath);
    std::vector<std::string> readLines(const std::string& filePath);
    void readLines(const std::string& filePath, std::vector<std::string>& lines);
    void readLines(const std::string& filePath, std::set<std::string>& lines);
    void copyStream(std::istream& is, std::ostream& os);
}