#pragma once

#include "logging/Level.h"
#include <string>
#include <chrono>
#include <string_view>

namespace logging {
    // TimePoint��ʾ����ʱ��㣬�Ǳ�׼��time_point����ʵ�ֵı���
    using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

    // Record�ඨ��
    class Record {
    public:
        // Logger����
        std::string name;
        // ��־�ȼ�
        Level level;
        // ��־ʱ��
        TimePoint time;
        // ��־��Ϣ
        std::string message;

        // getLevelName����ȡ��־�ȼ��ı�
        const std::string& getLevelName() const {
            // ����toLevelName��ȡ��־�ȼ��ı�
            return toLevelName(level);
        }
    };
}