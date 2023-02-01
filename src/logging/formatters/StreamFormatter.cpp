#include "logging/formatters/StreamFormatter.h"
#include "logging/Record.h"
#include <sstream>
#include <iomanip>

namespace logging::formatters::stream {
    // formatRecord����Record�����ʽ��Ϊ�ַ���
    std::string formatRecord(const Record& record) {
        // ��ȡʱ����е����ڣ�Year-Month-Day��
        const std::chrono::year_month_day ymd{ std::chrono::floor<std::chrono::days>(record.time) };
        // ��ȡʱ����е�ʱ�䣨Hour-Minute-Second��
        const std::chrono::hh_mm_ss hms{ record.time - std::chrono::floor<std::chrono::days>(record.time) };
        // �������������YYYY-mm-ddTHH:MM:SS.ms��ʽ���ı�
        std::ostringstream timeStringStream;
        timeStringStream << ymd << "T" << hms;

        // ��ȡʱ���ַ�������
        static constexpr std::size_t TIME_STRING_SIZE = std::size("YYYY-mm-ddTHH:MM:SS");
        // ��������еĵ����ַ�����ȡ������Ҫ���ı���YYYY-mm-ddTHH:MM:SS��������������
        std::string timeString = timeStringStream.str().substr(0, TIME_STRING_SIZE - 1) + "Z";

        // ������������ɸ�ʽ����־��Ϣ
        std::ostringstream logLineStream;
        // left����룬setw���ù̶����16
        logLineStream << std::left << std::setw(16) << record.name <<
            "| [" << record.getLevelName() << "] " <<
            timeString << " - " <<
            record.message;

        // ���ظ�ʽ�����ı�
        return logLineStream.str();
    }
}