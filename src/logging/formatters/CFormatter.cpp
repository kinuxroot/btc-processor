#include "logging/formatters/CFormatter.h"
#include "logging/Record.h"
#include <cstdint>
#include <ctime>
#include <cstring>

namespace logging::formatters::cstr {
    static std::string makeTimeString(time_t timeObj);

    std::string formatRecord(const Record& record) {
        static const int32_t LOG_LINE_BUFFER_SIZE = 4096;

        time_t timeObj = std::chrono::system_clock::to_time_t(record.time);
        std::string timeString = makeTimeString(timeObj);

        char logLineBuffer[LOG_LINE_BUFFER_SIZE];
        memset(logLineBuffer, 0, LOG_LINE_BUFFER_SIZE);
        snprintf(logLineBuffer, LOG_LINE_BUFFER_SIZE, "%-16s| [%s] %sZ - %s",
            record.name.c_str(),
            record.getLevelName().c_str(),
            timeString.c_str(),
            record.message.c_str()
        );

        return std::string(logLineBuffer);
    }

    static std::string makeTimeString(time_t timeObj) {
        static constexpr std::size_t TIME_BUFFER_SIZE = std::size("YYYY-mm-ddTHH:MM:SS");

        char timeBuffer[TIME_BUFFER_SIZE];
        memset(timeBuffer, 0, TIME_BUFFER_SIZE);

        std::strftime(std::data(timeBuffer), TIME_BUFFER_SIZE, "%Y-%m-%dT%H:%M:%S", std::localtime(&timeObj));

        return std::string(timeBuffer);
    }
}
