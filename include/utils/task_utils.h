#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <future>

#include "fmt/format.h"

namespace utils {
    template <class T>
    std::vector<std::vector<T>> generateTaskChunks(
        const std::vector<T>& taskList, uint32_t workerCount
    ) {
        std::vector<std::vector<T>> taskChunks(workerCount, std::vector<T>());

        size_t daysTotalCount = taskList.size();
        for (size_t dayIndex = 0; dayIndex != daysTotalCount; ++dayIndex) {
            auto taskIndex = dayIndex % workerCount;
            auto& taskChunk = taskChunks[taskIndex];

            taskChunk.push_back(taskList[dayIndex]);
        }

        return taskChunks;
    }

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