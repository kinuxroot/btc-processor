#include "btc_textify_union_find/logger.h"

LoggerType& getLogger() {
    using logging::LoggerFactory;
    using logging::Level;
    using logging::handlers::StreamHandler;
    using logging::handlers::FileHandler;
    using logging::formatters::cstr::formatRecord;

    static auto logger = LoggerFactory<Level::Debug>::createLogger("Textify Union Find", std::make_tuple(
        StreamHandler<Level::Debug>(formatRecord),
        FileHandler<Level::Debug>::create("logs/btc_textify_union_find.log", formatRecord)
    ));

    return logger;
}
