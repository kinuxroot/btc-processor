#pragma once

#include "logging/Handler.h"

namespace logging::handlers {
    template <Level HandlerLevel = Level::Warning>
    class DefaultHandler : public BaseHandler<HandlerLevel> {
    public:
        DefaultHandler(Formatter formatter = defaultFormatter) : BaseHandler<HandlerLevel>(formatter) {}
        DefaultHandler(const DefaultHandler&) = delete;
        DefaultHandler(const DefaultHandler&& rhs) noexcept : BaseHandler<HandlerLevel>(rhs.getForamtter()) {}

        template <Level emitLevel>
            requires (emitLevel > HandlerLevel)
        void emit(const Record& record) {
        }

        template <Level emitLevel>
            requires (emitLevel <= HandlerLevel)
        void emit(const Record& record) {
            std::cout << this->format(record) << std::endl;
        }
    };
}