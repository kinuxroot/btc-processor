#include "utils/json_utils.h"
#include "fmt/format.h"

#include <stdexcept>

namespace utils::json {
    nlohmann::json& get(nlohmann::json& jsonObj, const std::string& key) {
        auto jsonItem = jsonObj.find(key);
        if (jsonItem == jsonObj.end()) {
            throw std::out_of_range(fmt::format("Json object has no entry: {}", key));
        }

        return jsonObj[key];
    }

    const nlohmann::json& get(const nlohmann::json& jsonObj, const std::string& key) {
        auto jsonItem = jsonObj.find(key);
        if (jsonItem == jsonObj.end()) {
            throw std::out_of_range(fmt::format("Json object has no entry: {}", key));
        }

        return jsonObj[key];
    }
}