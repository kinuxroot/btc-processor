#include "logging/formatters/CFormatter.h"
#include "logging/Record.h"
#include <cstdint>
#include <ctime>

namespace logging::formatters::cstr {
    // makeTimeString����time_t�ṹ��ת�����ַ���
    static std::string makeTimeString(time_t timeObj);

    // formatRecord����Record�����ʽ��Ϊ�ַ���
    std::string formatRecord(const Record& record) {
        // LOG_LINE_BUFFER_SIZE��ʾ��ʽ����������󳤶�
        static const int32_t LOG_LINE_BUFFER_SIZE = 4096;

        // ��TimePoint����ת��Ϊ��Ӧ��C��׼time_t�ṹ��
        time_t timeObj = std::chrono::system_clock::to_time_t(record.time);
        // ����makeTimeString��time_t�ṹ��ת�����ַ���
        std::string timeString = makeTimeString(timeObj);

        // �����ʽ��������������ΪLOG_LINE_BUFFER_SIZE
        char logLineBuffer[LOG_LINE_BUFFER_SIZE];
        // �����������Ϊ0
        memset(logLineBuffer, 0, LOG_LINE_BUFFER_SIZE);
        // ����snprintf��ʽ�����������LOG_LINE_BUFFER_SIZE-1���ַ�����ֹ���������
        snprintf(logLineBuffer, LOG_LINE_BUFFER_SIZE, "%-16s| [%s] %sZ - %s",
            record.name.c_str(),
            record.getLevelName().c_str(),
            timeString.c_str(),
            record.message.c_str()
        );

        // ���ظ�ʽ������ַ���
        return std::string(logLineBuffer);
    }

    static std::string makeTimeString(time_t timeObj) {
        // ��ȡʱ�仺��������
        static constexpr std::size_t TIME_BUFFER_SIZE = std::size("YYYY-mm-ddTHH:MM:SS");

        // ����ʱ���ַ�����ʽ��������������ΪTIME_BUFFER_SIZE
        char timeBuffer[TIME_BUFFER_SIZE];
        // �����������Ϊ0
        memset(timeBuffer, 0, TIME_BUFFER_SIZE);

        // ����strftime���ʱ���ı���ʽ�����������TIME_BUFFER_SIZE-1���ַ�����ֹ���������
        std::strftime(std::data(timeBuffer), TIME_BUFFER_SIZE, "%Y-%m-%dT%H:%M:%S", std::localtime(&timeObj));

        // ���ظ�ʽ������ַ���
        return std::string(timeBuffer);
    }
}