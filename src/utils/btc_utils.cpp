#include "utils/btc_utils.h"

#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <cstdint>

namespace utils::btc {
    std::vector<std::string> loadId2Address(const char* filePath) {
        std::vector<std::string> id2Address;

        std::ifstream inputFile(filePath);
        if (!inputFile.is_open()) {
            std::cerr << "Id2Address file not found" << std::endl;

            return id2Address;
        }

        nlohmann::json id2AddressJson;
        inputFile >> id2AddressJson;

        if (!id2AddressJson.is_array()) {
            std::cerr << "Id2Address file is not json list" << std::endl;

            return id2Address;
        }

        for (const auto& addressJson : id2AddressJson) {
            std::string addressString = addressJson;
            id2Address.push_back(addressString);
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
