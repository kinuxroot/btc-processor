#pragma once

#include "logging/Formatter.h"
#include "logging/Level.h"
#include "logging/Record.h"
#include <string>
#include <memory>
#include <type_traits>
#include <concepts>

namespace logging {
    template <class HandlerType>
    concept Handler = requires (HandlerType handler, const Record & record, Level level) {
        handler.template emit<Level::Debug>(record);
        { handler.format(record) } -> std::same_as<std::string>;
    }&& std::move_constructible<HandlerType> && !std::copy_constructible<HandlerType>;

    template <Level HandlerLevel = Level::Warning>
    class BaseHandler {
    public:
        BaseHandler(Formatter formatter) : _formatter(formatter) {}

        BaseHandler(const BaseHandler&) = delete;
        BaseHandler& operator=(const BaseHandler&) = delete;

        BaseHandler(BaseHandler&& rhs) noexcept : _formatter(std::move(rhs._formatter)) {};

        virtual ~BaseHandler() {}

        Formatter getForamtter() const {
            return _formatter;
        }

        void setForamtter(Formatter formatter) {
            _formatter = formatter;
        }

        std::string format(const Record& record) {
            return _formatter(record);
        }

    private:
        Formatter _formatter;
    };
}
