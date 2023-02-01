#pragma once

#include "logging/Level.h"
#include <string>
#include <chrono>
#include <string_view>

namespace logging {
    using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

    class Record {
    public:
        std::string name;
        Level level;
        TimePoint time;
        std::string message;

        const std::string& getLevelName() const {
            return toLevelName(level);
        }
    };
}