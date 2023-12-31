#include "btc_gen_address_balance/logger.h"

LoggerType& getLogger() {
    using logging::LoggerFactory;
    using logging::Level;
    using logging::handlers::StreamHandler;
    using logging::handlers::FileHandler;
    using logging::formatters::cstr::formatRecord;

    static auto logger = LoggerFactory<Level::Debug>::createLogger("Generate address balance", std::make_tuple(
        StreamHandler<Level::Debug>(formatRecord),
        FileHandler<Level::Debug>::create("logs/btc_gen_addr_balance.log", formatRecord)
    ));

    return logger;
}
