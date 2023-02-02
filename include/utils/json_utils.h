#pragma

#include <nlohmann/json.hpp>
#include <string>

namespace utils::json {
    nlohmann::json& get(nlohmann::json& jsonObj, const std::string& key);
    const nlohmann::json& get(const nlohmann::json& jsonObj, const std::string& key);
}