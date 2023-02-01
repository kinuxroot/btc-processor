#pragma once

#include <string>
#include <vector>
#include <cstdint>

std::vector<std::vector<std::string>> generateTaskChunks(const std::vector<std::string>& daysDirList, uint32_t workerCount);