#include "final_export_entity_month_balance/logger.h"

LoggerType& getLogger() {
    using logging::LoggerFactory;
    using logging::Level;
    using logging::handlers::StreamHandler;
    using logging::handlers::FileHandler;
    using logging::formatters::cstr::formatRecord;

    static auto logger = LoggerFactory<Level::Debug>::createLogger("Generate entity month balance", std::make_tuple(
        StreamHandler<Level::Debug>(formatRecord)
    ));

    return logger;
}
