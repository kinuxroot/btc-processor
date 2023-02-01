#pragma once

#include <string>

namespace logging {
    class Record;

    namespace formatters::cstr {
        std::string formatRecord(const logging::Record& record);
    }
}