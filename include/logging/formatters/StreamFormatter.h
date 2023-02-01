#pragma once

#include <string>

namespace logging {
    class Record;

    namespace formatters::stream {
        std::string formatRecord(const logging::Record& record);
    }
}