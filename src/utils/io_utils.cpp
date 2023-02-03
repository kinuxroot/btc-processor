#include "btc-config.h"
#include "utils/io_utils.h"
#include "fmt/format.h"

#include <fstream>
#include <iostream>

namespace utils {
    std::string readFile(const std::string& filePath)
    {
        if (std::ifstream is{ "test.txt", std::ios::binary | std::ios::ate }) {
            auto size = is.tellg();
            std::string str(size, '\0');
            is.seekg(0);
            if (is.read(&str[0], size)) {
                return str;
            }
        }

        return "";
    }
    
    std::vector<std::string> readLines(const std::string& filePath) {
        std::vector<std::string> lines;
        std::ifstream inputFile(filePath);

        if (!inputFile.is_open()) {
            std::cerr << fmt::format("Can't open file {}", filePath) << std::endl;

            return lines;
        }

        while (inputFile) {
            std::string line;
            std::getline(inputFile, line);

            if (line.size() > 0) {
                lines.push_back(line);
            }
        }

        return lines;
    }

    void readLines(const std::string& filePath, std::vector<std::string>& lines) {
        std::ifstream inputFile(filePath);

        if (!inputFile.is_open()) {
            std::cerr << fmt::format("Can't open file {}", filePath) << std::endl;

            return;
        }

        while (inputFile) {
            std::string line;
            std::getline(inputFile, line);

            if (line.size() > 0) {
                lines.push_back(line);
            }
        }
    }

    void copyStream(std::istream& is, std::ostream& os) {
        is.seekg(0, std::ios::end);
        auto size = is.tellg();
        std::string str(size, '\0'); // construct string to stream size

        is.seekg(0);
        if (is.read(&str[0], size)) {
            os.write(str.c_str(), size);
        }
    }
}
