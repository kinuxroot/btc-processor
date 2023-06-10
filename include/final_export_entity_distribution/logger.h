#pragma once

#include "logging/Logger.h"
#include "logging/formatters/CFormatter.h"
#include "logging/handlers/StreamHandler.h"
#include "logging/handlers/FileHandler.h"

using LoggerType = decltype(logging::LoggerFactory<logging::Level::Debug>::createLogger("Generate Distribution", std::make_tuple(
    logging::handlers::StreamHandler<logging::Level::Debug>(logging::formatters::cstr::formatRecord),
    logging::handlers::FileHandler<logging::Level::Debug>("btc_gen_entity_distribution.log", logging::formatters::cstr::formatRecord)
)));


LoggerType& getLogger();
