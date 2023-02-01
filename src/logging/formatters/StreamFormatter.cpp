#include "logging/formatters/StreamFormatter.h"
#include "logging/Record.h"
#include <sstream>
#include <iomanip>

namespace logging::formatters::stream {
    std::string formatRecord(const Record& record) {
        const std::chrono::year_month_day ymd{ std::chrono::floor<std::chrono::days>(record.time) };
        const std::chrono::hh_mm_ss hms{ record.time - std::chrono::floor<std::chrono::days>(record.time) };
        std::ostringstream timeStringStream;
        timeStringStream << ymd << "T" << hms;

        static constexpr std::size_t TIME_STRING_SIZE = std::size("YYYY-mm-ddTHH:MM:SS");
        std::string timeString = timeStringStream.str() + "Z";

        std::ostringstream logLineStream;
        logLineStream << std::left << std::setw(16) << record.name <<
            "| [" << record.getLevelName() << "] " <<
            timeString << " - " <<
            record.message;

        return logLineStream.str();
    }
}