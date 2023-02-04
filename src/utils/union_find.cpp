#include "utils/union_find.h"

namespace utils::btc {
    namespace fs = std::filesystem;

    WeightedQuickUnion::WeightedQuickUnion(BtcSize count) :
        _ids(count, 0), _sizes(count, 1), _count(count) {
        BtcSize currentId = 0;
        for (auto& id : _ids) {
            id = currentId;

            ++currentId;
        }
    }

    void WeightedQuickUnion::connected(BtcId p, BtcId q) {
    }

    BtcId WeightedQuickUnion::find(BtcId p) {
        return 0;
    }

    void WeightedQuickUnion::connect(BtcId p, BtcId q) {
    }

    void WeightedQuickUnion::save(const fs::path& path) {

    }

    void WeightedQuickUnion::load(const fs::path& path) {

    }
}