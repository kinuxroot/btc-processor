#pragma once

#include "Record.h"
#include <functional>
#include <string>
#include <format>

namespace logging {
    // Formatter���ͣ����н���־��¼ת�����ַ����ĺ���/�º�����Lambda��������ΪFormatter
    using Formatter = std::function<std::string(const Record& record)>;

    // defaultFormatter��Ĭ�ϸ�ʽ����
    inline std::string defaultFormatter(const Record& record) {
        // ����format���ؼ򵥵���־��Ϣ
        return std::format("{}:{}:{}", record.getLevelName(), record.name, record.message);
    }
}