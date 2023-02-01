#pragma once

#include <iostream>
#include <cstdint>
#include <string>
#include <vector>

namespace logging {
    // ��־�ȼ�����Ϊö�٣�ö��ֵʵ�����Ͷ���Ϊint8_t���ȼ�Խ����ֵԽС
    enum class Level : int8_t {
        // ���ش��󣬵ȼ����
        Critical = 0,
        // һ�����
        Error = 1,
        // ������Ϣ��һ�㲻�����������ж�
        Warning = 2,
        // ��ͨ��Ϣ
        Info = 3,
        // ������Ϣ��������Խ׶�ʹ��
        Debug = 4,
        // ���������Ϣ��������Խ׶�ʹ�ã��ȼ����
        Trace = 5,
    };

    // toLevelName����Levelö��ת�����ı�
    inline const std::string& toLevelName(Level level)
    {
        // ��־�ȼ���ȼ��ı���ӳ���
        static std::vector<std::string> LevelNames{
            "CRITICAL",
            "ERROR",
            "WARN",
            "INFO",
            "DEBUG",
            "TRACE",
        };

        // ʹ��static_cast����־�ȼ�ת���ɶ�Ӧ��ֵ
        // Ȼ���ӳ����л�ȡ��Ӧ�ĵȼ��ı�
        return LevelNames.at(static_cast<int8_t>(level));
    }

    // operator<<�����Level
    inline std::ostream& operator<<(std::ostream& os, Level level)
    {
        // ��ȡLevel�ı�����������������
        os << toLevelName(level);

        return os;
    }
}