#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <future>

#include "fmt/format.h"

namespace utils {
    std::vector<std::vector<std::string>> generateTaskChunks(const std::vector<std::string>& daysDirList, uint32_t workerCount);

    template <class T, class Logger>
    void waitForTasks(Logger& logger, std::vector<std::future<T>>& tasks) {
        int32_t taskIndex = 0;
        for (auto& task : tasks) {
            logger.info(fmt::format("Wait for task stop: {}", taskIndex));
            task.wait();

            ++taskIndex;
        }
    }
}