#pragma once

#include <string>

namespace logging {
    class Record;

    namespace formatters::modern {
        // formatRecord�������ڸ�ʽ����־��¼����
        std::string formatRecord(const logging::Record& record);
    }
}