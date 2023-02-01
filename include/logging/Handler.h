#pragma once

#include "logging/Formatter.h"
#include "logging/Level.h"
#include "logging/Record.h"
#include <string>
#include <memory>
#include <type_traits>
#include <concepts>

namespace logging {
    // Handler Concept
    // ��ǿ������Handler���̳�BaseHandler��ֻ��Ҫ�����ض��Ľӿڣ���˶���Concept
    template <class HandlerType>
    concept Handler = requires (HandlerType handler, const Record & record, Level level) {
        // Ҫ����emit��Ա����
        handler.emit;
        // Ҫ����format���������Խ�Record�����ʽ��Ϊstring���͵��ַ���
        { handler.format(record) } -> std::same_as<std::string>;
        // Ҫ�����ƶ����캯�����޿������캯��
    }&& std::move_constructible<HandlerType> && !std::copy_constructible<HandlerType>;

    // BaseHandler�ඨ��
    // HandlerLevel����־����������־�ȼ�
    // �Լ�ʵ��Handlerʱ���Լ̳�BaseHandlerȻ��ʵ��emit
    template <Level HandlerLevel = Level::Warning>
    class BaseHandler {
    public:
        // ���캯����formatterΪ��־�������ĸ�ʽ����
        BaseHandler(Formatter formatter) : _formatter(formatter) {}

        // ��������
        BaseHandler(const BaseHandler&) = delete;
        // ������ֵ
        BaseHandler& operator=(const BaseHandler&) = delete;

        // �ƶ����캯����������־����������֮���ƶ�
        BaseHandler(BaseHandler&& rhs) noexcept : _formatter(std::move(rhs._formatter)) {};

        // �������������ǵ��ᱻ�̳У���������ʱ������Դй¶
        virtual ~BaseHandler() {}

        // getForamtter����ȡformatter
        Formatter getForamtter() const {
            return _formatter;
        }

        // setForamtter���޸�formatter
        void setForamtter(Formatter formatter) {
            _formatter = formatter;
        }

        // format�����ø�ʽ������recordת�����ı��ַ���
        std::string format(const Record& record) {
            return _formatter(record);
        }

    private:
        // ��־�������ĸ�ʽ����
        Formatter _formatter;
    };
}