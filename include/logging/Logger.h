#pragma once

#include <iostream>
#include <string>
#include <tuple>
#include <memory>
#include "logging/Level.h"
#include "logging/Handler.h"
#include "logging/handlers/DefaultHandler.h"

namespace logging {
    template <Level loggerLevel, Handler... HandlerTypes>
        requires(sizeof...(HandlerTypes) > 0)
    class Logger {
    public:
        static constexpr int32_t HandlerCount = sizeof...(HandlerTypes);
        static constexpr Level LoggerLevel = loggerLevel;

        Logger(const std::string& name, std::tuple<HandlerTypes...>&& attachedHandlers) :
            _name(name), _attachedHandlers(std::forward<std::tuple<HandlerTypes...>>(attachedHandlers)) {
        }

        Logger(const Logger&) = delete;
        Logger& operator=(const Logger&) = delete;

        Logger(Logger&& rhs) :
            _name(std::move(rhs._name)), _attachedHandlers(std::move(rhs._attachedHandlers)) {
        }

        template <Level level>
            requires (level > loggerLevel)
        Logger& log(const std::string& message) {
            return *this;
        }

        template <Level level>
            requires (level <= loggerLevel)
        Logger & log(const std::string& message) {
            Record record{
                .name = _name,
                .level = level,
                .time = std::chrono::system_clock::now(),
                .message = message,
            };

            handleLog<level, HandlerCount - 1>(record);

            return *this;
        }

        template <Level messageLevel, int32_t handlerIndex>
            requires (handlerIndex > 0)
        void handleLog(const Record& record) {
            handleLog<messageLevel, handlerIndex - 1>(record);

            auto& handler = std::get<handlerIndex>(_attachedHandlers);
            handler.emit<messageLevel>(record);
        }

        template <Level messageLevel, int32_t handlerIndex>
            requires (handlerIndex == 0)
        void handleLog(const Record& record) {
            auto& handler = std::get<handlerIndex>(_attachedHandlers);
            handler.emit<messageLevel>(record);
        }

        Logger& critical(const std::string& message) {
            return log<Level::Critical>(message);
        }

        Logger& error(const std::string& message) {
            return log<Level::Error>(message);
        }

        Logger& warning(const std::string& message) {
            return log<Level::Warning>(message);
        }

        Logger& info(const std::string& message) {
            return log<Level::Info>(message);
        }

        Logger& debug(const std::string& message) {
            return log<Level::Debug>(message);
        }

        Logger& trace(const std::string& message) {
            return log<Level::Trace>(message);
        }

    private:
        std::string _name;
        std::tuple<HandlerTypes...> _attachedHandlers;
    };

    template <Level level = Level::Warning>
    class LoggerFactory {
    public:
        template <Handler... HandlerTypes>
        static Logger<level, HandlerTypes...> createLogger(const std::string& name, std::tuple<HandlerTypes...>&& attachedHandlers) {
            return Logger<level, HandlerTypes...>(name, std::forward<std::tuple<HandlerTypes...>>(attachedHandlers));
        }

        template <Handler... HandlerTypes>
        static Logger<level, handlers::DefaultHandler<level>> createLogger(const std::string& name) {
            return Logger<level, handlers::DefaultHandler<level>>(name, std::make_tuple(handlers::DefaultHandler<level>()));
        }
    };
}