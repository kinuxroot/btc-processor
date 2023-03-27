#include "btc_match_exchange_address/logger.h"

LoggerType& getLogger() {
    using logging::LoggerFactory;
    using logging::Level;
    using logging::handlers::StreamHandler;
    using logging::handlers::FileHandler;
    using logging::formatters::cstr::formatRecord;

    static auto logger = LoggerFactory<Level::Debug>::createLogger("Match Exchange Address", std::make_tuple(
        StreamHandler<Level::Debug>(formatRecord)
    ));

    return logger;
}
