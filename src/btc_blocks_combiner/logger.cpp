#include "btc_blocks_combiner/logger.h"

LoggerType& getLogger() {
    using logging::LoggerFactory;
    using logging::Level;
    using logging::handlers::StreamHandler;
    using logging::handlers::FileHandler;
    using logging::formatters::stream::formatRecord;

    static auto logger = LoggerFactory<Level::Debug>::createLogger("Root", std::make_tuple(
        StreamHandler<Level::Debug>(logging::formatters::stream::formatRecord),
        FileHandler<Level::Debug>("btc_combine_blocks.log", logging::formatters::stream::formatRecord)
    ));

    return logger;
}