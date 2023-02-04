#pragma once

#include "btc_utils.h"
#include <cstdint>
#include <vector>
#include <filesystem>
#include <iostream>

namespace utils::btc {
    using BtcSize = BtcId;

    class WeightedQuickUnion;

    std::ostream& operator<<(std::ostream& os, const WeightedQuickUnion& quickUnion);

    class WeightedQuickUnion {
    public:
        friend std::ostream& operator<<(std::ostream& os, const WeightedQuickUnion& quickUnion);

        WeightedQuickUnion(BtcSize idCount);

        bool connected(BtcId p, BtcId q);
        BtcId findRoot(BtcId p);
        void connect(BtcId p, BtcId q);
        void merge(const WeightedQuickUnion& rhs);
        void save(const std::filesystem::path& path) const;
        void load(const std::filesystem::path& path);

        BtcSize getClusterCount() const {
            return _clusterCount;
        }

        bool operator==(const WeightedQuickUnion& rhs) const {
            return _ids == rhs._ids &&
                _sizes == rhs._sizes &&
                _clusterCount == _clusterCount;
        }

    private:
        std::vector<BtcId> _ids;
        std::vector<BtcSize> _sizes;
        BtcSize _clusterCount;
    };
};