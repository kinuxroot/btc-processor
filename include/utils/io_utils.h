#pragma once

#include <string>

namespace utils {
    std::string readFile(const std::string& filePath);
    void copyStream(std::istream& is, std::ostream& os);
}