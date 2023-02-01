#pragma once

#include "logging/Handler.h"

namespace logging::handlers {
    // Ĭ����־������
    template <Level HandlerLevel = Level::Warning>
    // �̳�BaseHandler
    class DefaultHandler : public BaseHandler<HandlerLevel> {
    public:
        // ���캯������Ҫָ����ʽ������Ĭ�ϸ�ʽ����ΪdefaultFormatter
        DefaultHandler(Formatter formatter = defaultFormatter) : BaseHandler<HandlerLevel>(formatter) {}
        // ��ֹ�������캯��
        DefaultHandler(const DefaultHandler&) = delete;
        // �����ƶ����캯��
        DefaultHandler(const DefaultHandler&& rhs) noexcept : BaseHandler<HandlerLevel>(rhs.getForamtter()) {}

        // emit�����ύ��־��¼
        // emitLevel > HandlerLevel����־�ᱻ����
        template <Level emitLevel>
            requires (emitLevel > HandlerLevel)
        void emit(const Record& record) {
        }

        // emitLevel <= HandlerLevel����־�ᱻ�������׼�������
        template <Level emitLevel>
            requires (emitLevel <= HandlerLevel)
        void emit(const Record& record) {
            // ����format����־��¼�����ʽ�����ı��ַ���
            std::cout << this->format(record) << std::endl;
        }
    };
}