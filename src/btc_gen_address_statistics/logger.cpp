#include "btc_gen_address_statistics/logger.h"

LoggerType& getLogger() {
    using logging::LoggerFactory;
    using logging::Level;
    using logging::handlers::StreamHandler;
    using logging::handlers::FileHandler;
    using logging::formatters::cstr::formatRecord;

    static auto logger = LoggerFactory<Level::Debug>::createLogger("Generate address statistics", std::make_tuple(
        StreamHandler<Level::Debug>(formatRecord)
    ));

    return logger;
}
