#pragma once

#include "btc_utils.h"
#include <cstdint>
#include <vector>
#include <filesystem>
#include <iostream>
#include <functional>

namespace utils::btc {
    using BtcSize = BtcId;

    class WeightedQuickUnion;

    std::ostream& operator<<(std::ostream& os, const WeightedQuickUnion& quickUnion);
    
    class WeightedQuickUnionClusters;

    class WeightedQuickUnion {
    public:
        friend std::ostream& operator<<(std::ostream& os, const WeightedQuickUnion& quickUnion);
        friend class WeightedQuickUnionClusters;

        WeightedQuickUnion(BtcSize idCount);

        bool connected(BtcId p, BtcId q);
        BtcId findRoot(BtcId p) const;
        void connect(BtcId p, BtcId q);
        void merge(const WeightedQuickUnion& rhs);
        void save(const std::filesystem::path& path) const;
        void load(const std::filesystem::path& path);
        void resize(BtcSize newSize);

        BtcSize getClusterCount() const {
            return _clusterCount;
        }

        BtcSize getSize() const {
            return _ids.size();
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

    class WeightedQuickUnionClusters {
    public:
        using ForEachFunc = std::function<void(BtcId, BtcSize)>;

        WeightedQuickUnionClusters(const WeightedQuickUnion& quickUnion);

        void forEach(ForEachFunc handler) const;

    private:
        const WeightedQuickUnion& _quickUnion;
    };
};