#pragma once

#include <string>

namespace logging {
    class Record;

    namespace formatters::stream {
        // formatRecord�������ڸ�ʽ����־��¼����
        std::string formatRecord(const logging::Record& record);
    }
}