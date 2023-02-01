#pragma once

#include <string>
#include <vector>

namespace utils {
    std::string readFile(const std::string& filePath);
    std::vector<std::string> readLines(const std::string& filePath);
    void copyStream(std::istream& is, std::ostream& os);
}