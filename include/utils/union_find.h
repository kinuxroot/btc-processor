#pragma once

#include "btc_utils.h"
#include <cstdint>
#include <vector>
#include <filesystem>

namespace utils::btc {
    using BtcSize = BtcId;

    class WeightedQuickUnion {
    public:
        WeightedQuickUnion(BtcSize count);

        void connected(BtcId p, BtcId q);
        BtcId find(BtcId p);
        void connect(BtcId p, BtcId q);
        void save(const std::filesystem::path& path);
        void load(const std::filesystem::path& path);

        BtcSize getCount() const {
            return _count;
        }

    private:
        std::vector<BtcId> _ids;
        std::vector<BtcSize> _sizes;
        BtcSize _count;
    };
};