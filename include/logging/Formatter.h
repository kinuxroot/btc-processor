#pragma once

#include "Record.h"
#include <functional>
#include <string>

namespace logging {
    using Formatter = std::function<std::string(const Record& record)>;

    inline std::string defaultFormatter(const Record& record) {
        return record.getLevelName() + ":" + record.name + ":" + record.message;
    }
}
