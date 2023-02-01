#pragma once

#include "logging/Handler.h"
#include <fstream>
#include <string>
#include <syncstream>

namespace logging::handlers {
    template <Level HandlerLevel = Level::Warning>
    class FileHandler : public BaseHandler<HandlerLevel> {
    public:
        FileHandler(const std::string filePath, Formatter formatter = defaultFormatter) :
            BaseHandler<HandlerLevel>(formatter),
            _stream(filePath.c_str()), _syncStream(_stream) {
        }

        FileHandler(const std::string filePath, std::ios_base::openmode mode, Formatter formatter = defaultFormatter) :
            BaseHandler<HandlerLevel>(formatter),
            _stream(filePath.c_str(), mode), _syncStream(_stream) {
        }

        FileHandler(const FileHandler&) = delete;
        FileHandler(FileHandler&& rhs) noexcept : 
            BaseHandler<HandlerLevel>(rhs.getForamtter()), _stream(std::move(rhs._stream)), _syncStream(_stream) {}

        template <Level emitLevel>
            requires (emitLevel <= HandlerLevel)
        void emit(const Record& record) {
            _syncStream << this->format(record) << std::endl;
        }

        template <Level emitLevel>
            requires (emitLevel > HandlerLevel)
        void emit(const Record& record) {
        }

    private:
        std::ofstream _stream;
        std::osyncstream _syncStream;
    };
}