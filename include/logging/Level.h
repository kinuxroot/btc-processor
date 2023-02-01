#pragma once

#include <iostream>
#include <cstdint>
#include <string>
#include <vector>

namespace logging {
    enum class Level : int8_t {
        Critical = 0,
        Error = 1,
        Warning = 2,
        Info = 3,
        Debug = 4,
        Trace = 5,
    };

    inline const std::string& toLevelName(Level level)
    {
        static std::vector<std::string> LevelNames{
            "CRITICAL",
            "ERROR",
            "WARN",
            "INFO",
            "DEBUG",
            "TRACE",
        };

        return LevelNames.at(static_cast<int8_t>(level));
    }

    inline std::ostream& operator<<(std::ostream& os, Level level)
    {
        os << toLevelName(level);

        return os;
    }
}