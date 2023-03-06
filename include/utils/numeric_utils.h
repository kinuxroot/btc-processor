#pragma once

#include "fmt/format.h"

#include <vector>
#include <cmath>
#include <limits>

namespace utils {
    template <typename Container, typename T>
    std::size_t binaryFindRangeLow(
        const Container& container,
        T value,
        std::size_t begin,
        std::size_t end
    ) {
        if (begin == end || begin + 1 == end) {
            return begin;
        }

        std::size_t mid = (begin + end) / 2;
        if (value >= container[begin] && value < container[mid]) {
            return binaryFindRangeLow(container, value, begin, mid);
        }

        return binaryFindRangeLow(container, value, mid, end);
    }

    template <typename T>
    std::vector<T> range(T start, T end, T step) {
        if (step == 0) {
            std::cerr << "Range step can not be 0" << std::endl;

            return std::vector<T>();
        }
        else if (step > 0 && end < start) {
            std::cerr << fmt::format(
                "Range end {} must be greater than start {} when step is {}", end, start, step
            ) << std::endl;

            return std::vector<T>();
        }
        else if (step < 0 && end > start) {
            std::cerr << fmt::format(
                "Range end {} must be less than start {} when step is {}", end, start, step
            ) << std::endl;

            return std::vector<T>();
        }

        std::size_t sizeCount = static_cast<std::size_t>(ceil((end - start) / step));
        std::vector<T> generatedRange(sizeCount, start);
        T currentValue = start;

        for (auto& generatedValue : generatedRange) {
            generatedValue = currentValue;
            currentValue += step;
        }

        return generatedRange;
    }
}