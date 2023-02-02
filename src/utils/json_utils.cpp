#include "utils/json_utils.h"
#include "fmt/format.h"

namespace utils::json {
    nlohmann::json& get(nlohmann::json& jsonObj, const std::string& key) {
        auto jsonItem = jsonObj.find(key);
        if (jsonItem != jsonObj.end()) {
            throw fmt::format("Json object has no entry: {}", key);
        }

        return jsonObj[key];
    }

    const nlohmann::json& get(const nlohmann::json& jsonObj, const std::string& key) {
        auto jsonItem = jsonObj.find(key);
        if (jsonItem != jsonObj.end()) {
            throw fmt::format("Json object has no entry: {}", key);
        }

        return jsonObj[key];
    }
}