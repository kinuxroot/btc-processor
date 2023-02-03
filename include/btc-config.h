#pragma once

#include <cstddef>
#include <cstdint>

// 800MB
const std::size_t COMBINED_BLOCK_FILE_SIZE = 800 * 1024 * 1024;
const std::size_t FILE_READ_BUFFER_SIZE = 10 * 1024;

// Worker counts
const uint32_t BTC_COMBINE_BLOCKS_WORKER_COUNT = 16;
const uint32_t BTC_GEN_ADDRESS_WORKER_COUNT = 32;
const uint32_t BTC_GEN_DAY_INS_WORKER_COUNT = 64;

