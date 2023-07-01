#include "final_export_union_find/logger.h"

LoggerType& getLogger() {
    using logging::LoggerFactory;
    using logging::Level;
    using logging::handlers::StreamHandler;
    using logging::handlers::FileHandler;
    using logging::formatters::cstr::formatRecord;

    static auto logger = LoggerFactory<Level::Debug>::createLogger("Export Union Find Addresses", std::make_tuple(
        StreamHandler<Level::Debug>(formatRecord)
    ));

    return logger;
}
