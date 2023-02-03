#include "utils/btc_utils.h"
#include "fmt/format.h"

#include <fstream>
#include <iostream>
#include <cstdint>
#include <string>

namespace utils::btc {
    std::size_t ADDRESS_LENGTH = 32;
    std::size_t WRITE_BUFFER_ELEMENT_COUNT = 1024 * 1024;
    std::size_t WRITE_BUFFER_SIZE = ADDRESS_LENGTH * WRITE_BUFFER_ELEMENT_COUNT;

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

    void loadId2Address(const char* filePath, std::vector<std::string>& id2Address) {
        std::ifstream inputFile(filePath);
        if (!inputFile.is_open()) {
            std::cerr << "Id2Address file not found" << std::endl;

            return;
        }

        while (inputFile) {
            std::string line;
            std::getline(inputFile, line);

            if (!line.size()) {
                break;
            }

            id2Address.push_back(line);
        }
    }

    void dumpId2Address(const char* filePath, const std::set<std::string>& id2Address) {
        std::cout << fmt::format("Dump i2daddr: {}", filePath) << std::endl;

        std::ofstream id2AddressFile(filePath);

        size_t bufferedCount = 0;
        size_t writtenCount = 0;

        std::string writeBuffer;
        writeBuffer.reserve(WRITE_BUFFER_SIZE);

        for (const auto& address : id2Address) {
            writeBuffer.append(address).append("\n");
            ++bufferedCount;

            if (bufferedCount && bufferedCount % WRITE_BUFFER_ELEMENT_COUNT == 0) {
                id2AddressFile << writeBuffer;
                writeBuffer.clear();
                writtenCount = bufferedCount;

                std::cout << fmt::format("Written {} addresses", writtenCount) << std::endl;
            }
        }

        if (writtenCount < bufferedCount) {
            id2AddressFile << writeBuffer;
            writeBuffer.clear();
            writtenCount = bufferedCount;

            std::cout << fmt::format("Written {} addresses", writtenCount) << std::endl;
        }
    }

    std::map<std::string, BtcId> generateAddress2Id(const std::vector<std::string>& id2Address) {
        std::map<std::string, BtcId> address2Id;

        uint32_t addressId = 0;
        for (const auto& address : id2Address) {
            address2Id[address] = addressId;

            ++addressId;
        }

        return address2Id;
    }

    std::map<std::string, BtcId> loadAddress2Id(const char* filePath) {
        std::map<std::string, BtcId> address2Id;

        std::ifstream inputFile(filePath);
        if (!inputFile.is_open()) {
            std::cerr << "Id2Address file not found" << std::endl;

            return address2Id;
        }

        BtcId addressId = 0;
        while (inputFile) {
            std::string line;
            std::getline(inputFile, line);

            if (!line.size()) {
                break;
            }

            address2Id[line] = addressId;
            ++addressId;
        }

        return address2Id;
    }
}
