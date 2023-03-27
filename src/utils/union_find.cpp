#include "utils/union_find.h"
#include "fmt/format.h"

#include <fstream>

namespace utils::btc {
    namespace fs = std::filesystem;

    WeightedQuickUnion::WeightedQuickUnion(BtcSize idCount) :
        _ids(idCount, 0), _sizes(idCount, 1), _clusterCount(idCount) {
        BtcSize currentId = 0;
        for (auto& id : _ids) {
            id = currentId;

            ++currentId;
        }
    }

    bool WeightedQuickUnion::connected(BtcId p, BtcId q) {
        return findRoot(p) == findRoot(q);
    }

    BtcId WeightedQuickUnion::findRoot(BtcId p) const {
        while (p != _ids[p]) {
            p = _ids[p];
        }

        return p;
    }

    BtcSize WeightedQuickUnion::getClusterSize(BtcId p) const {
        auto pRoot = _ids[p];
        if (pRoot != p) {
            return 0;
        }

        return _sizes[p];
    }

    void WeightedQuickUnion::connect(BtcId p, BtcId q) {
        auto pRoot = findRoot(p);
        auto qRoot = findRoot(q);

        if (pRoot != qRoot) {
            // Balance insert
            if (_sizes[pRoot] < _sizes[qRoot]) {
                // Insert p to q
                _ids[pRoot] = qRoot;
                _sizes[qRoot] += _sizes[pRoot];
            }
            else {
                // Insert q to p
                _ids[qRoot] = pRoot;
                _sizes[pRoot] += _sizes[qRoot];
            }

            -- _clusterCount;
        }
    }

    void WeightedQuickUnion::merge(const WeightedQuickUnion& rhs) {
        BtcId maxId = rhs._ids.size();

        for (BtcId p = 0; p != maxId; ++p) {
            auto q = rhs._ids[p];
            if (p != q) {
                connect(p, q);
            }
        }
    }

    void WeightedQuickUnion::save(const fs::path& path) const {
        std::ofstream outputFile(path.c_str(), std::ios::binary);

        outputFile.write(reinterpret_cast<const char*>(&_clusterCount), sizeof(_clusterCount));

        BtcId maxId = _ids.size();
        outputFile.write(reinterpret_cast<const char*>(&maxId), sizeof(BtcId));
        outputFile.write(reinterpret_cast<const char*>(_ids.data()), _ids.size() * sizeof(BtcId));
        outputFile.write(reinterpret_cast<const char*>(_sizes.data()), _sizes.size() * sizeof(BtcSize));
    }

    void WeightedQuickUnion::load(const fs::path& path) {
        std::ifstream inputFile(path.c_str(), std::ios::binary);

        inputFile.read(reinterpret_cast<char*>(&_clusterCount), sizeof(_clusterCount));

        BtcId maxId = 0;
        inputFile.read(reinterpret_cast<char*>(&maxId), sizeof(BtcId));

        _ids.resize(maxId);
        inputFile.read(reinterpret_cast<char*>(_ids.data()), _ids.size() * sizeof(BtcId));
        _sizes.resize(maxId);
        inputFile.read(reinterpret_cast<char*>(_sizes.data()), _sizes.size() * sizeof(BtcSize));
    }

    void WeightedQuickUnion::resize(BtcSize newSize) {
        auto originalSize = getSize();
        if (originalSize >= newSize) {
            return;
        }

        _ids.resize(newSize);
        _sizes.resize(newSize);

        _clusterCount += newSize - originalSize;

        for (BtcId currentId = originalSize; originalSize < newSize; ++originalSize) {
            _ids[currentId] = currentId;
            _sizes[currentId] = 1;
        }
    }

    std::ostream& operator<<(std::ostream& os, const WeightedQuickUnion& quickUnion) {
        os << fmt::format("Cluster count: {}", quickUnion._clusterCount) << std::endl;

        os << "Ids size:" << quickUnion._ids.size() << std::endl;
        os << "Ids:" << std::endl;

        os << "Sizes size:" << quickUnion._sizes.size() << std::endl;
        os << "Sizes:" << std::endl;

        return os;
    }

    WeightedQuickUnionClusters::WeightedQuickUnionClusters(const WeightedQuickUnion& quickUnion) :
        _quickUnion(quickUnion) {}

    void WeightedQuickUnionClusters::forEach(ForEachFunc handler) const {
        BtcSize currentId = 0;

        for (auto currentParent : _quickUnion._ids) {
            auto currentSize = _quickUnion._sizes[currentId];

            if (currentParent == currentId) {
                handler(currentId, currentSize);
            }

            ++currentId;
        }
    }

}