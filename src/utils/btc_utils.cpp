#include "utils/btc_utils.h"

#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <cstdint>
#include <string>

namespace utils::btc {
    std::vector<std::string> loadId2Address(const char* filePath) {
        std::vector<std::string> id2Address;

        std::ifstream inputFile(filePath);
        if (!inputFile.is_open()) {
            std::cerr << "Id2Address file not found" << std::endl;

            return id2Address;
        }
        
        while (inputFile) {
            std::string line;
            std::getline(inputFile, line);

            if (!line.size()) {
                break;
            }

            id2Address.push_back(line);
        }

        return id2Address;
    }

    std::map<std::string, std::size_t> generateAddress2Id(const std::vector<std::string>& id2Address) {
        std::map<std::string, std::size_t> address2Id;

        size_t addressId = 0;
        for (const auto& address : id2Address) {
            address2Id[address] = addressId;

            ++addressId;
        }

        return address2Id;
    }
}
