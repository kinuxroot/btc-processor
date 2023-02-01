#include "utils/task_utils.h"

std::vector<std::vector<std::string>> generateTaskChunks(const std::vector<std::string>& daysDirList, uint32_t workerCount) {
    std::vector<std::vector<std::string>> taskChunks(workerCount, std::vector<std::string>());

    size_t daysTotalCount = daysDirList.size();
    for (size_t dayIndex = 0; dayIndex != daysTotalCount; ++dayIndex) {
        auto taskIndex = dayIndex % workerCount;
        auto& taskChunk = taskChunks[taskIndex];

        taskChunk.push_back(daysDirList[dayIndex]);
    }

    return taskChunks;
}