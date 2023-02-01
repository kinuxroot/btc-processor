#include "btc_blocks_combiner/logger.h"

LoggerType& getLogger() {
    using logging::LoggerFactory;
    using logging::Level;
    using logging::handlers::StreamHandler;
    using logging::handlers::FileHandler;
    using logging::formatters::cstr::formatRecord;

    static auto logger = LoggerFactory<Level::Debug>::createLogger("Combine Blocks", std::make_tuple(
        StreamHandler<Level::Debug>(formatRecord),
        FileHandler<Level::Debug>("btc_combine_blocks.log", std::ios::app, formatRecord)
    ));

    return logger;
}
