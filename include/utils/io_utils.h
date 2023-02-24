#pragma once

#include "fmt/format.h"

#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <fstream>
#include <functional>

namespace utils {
    std::string readFile(const std::string& filePath);
    
    std::vector<std::string> readLines(const std::string& filePath);
    void readLines(const std::string& filePath, std::vector<std::string>& lines);
    void readLines(const std::string& filePath, std::set<std::string>& lines);

    template <typename T>
    std::vector<T> readLines(
        const std::string& filePath,
        std::function<T(const std::string&)> converter
    ) {
        std::vector<std::string> lines;
        readLines(filePath, lines);

        std::vector<T> results;
        for (const auto& line : lines) {
            results.push_back(converter(line));
        }

        return results;
    }

    void copyStream(std::istream& is, std::ostream& os);

    template <typename T>
    void writeLines(const std::string& filePath, const std::vector<T>& lines) {
        std::ofstream outputFile(filePath);

        if (!outputFile.is_open()) {
            std::cerr << fmt::format("Can't open file {}", filePath) << std::endl;

            return;
        }

        for (const auto& line : lines) {
            outputFile << line << std::endl;
        }
    }
}
