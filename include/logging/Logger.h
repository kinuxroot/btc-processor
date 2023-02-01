#pragma once

#include <iostream>
#include <string>
#include <tuple>
#include <memory>
#include "logging/Level.h"
#include "logging/Handler.h"
#include "logging/handlers/DefaultHandler.h"

namespace logging {
    // Logger�ඨ��
    // Level����־��¼������־�ȼ�
    // HandlerTypes������ע�����־����������������HandlerԼ��
    // ͨ��requiresҪ��ÿ��Logger�����ע������һ����־������
    template <Level loggerLevel, Handler... HandlerTypes>
        requires(sizeof...(HandlerTypes) > 0)
    class Logger {
    public:
        // HandlerCount����־��¼��������ͨ��sizeof...��ȡģ������в�������������
        static constexpr int32_t HandlerCount = sizeof...(HandlerTypes);
        // LoggerLevel��Logger����־�ȼ�
        static constexpr Level LoggerLevel = loggerLevel;

        // ���캯����nameΪ��־��¼�����ƣ�attachedHandlers����Ҫע�ᵽLogger�����е���־������
        // ������־������Ҳ��������ֻ�����ƶ�����������õ���Ԫ����ƶ����캯��
        Logger(const std::string& name, std::tuple<HandlerTypes...>&& attachedHandlers) :
            // ����std::forwardת����ֵ����
            _name(name), _attachedHandlers(std::forward<std::tuple<HandlerTypes...>>(attachedHandlers)) {
        }

        // ��������
        Logger(const Logger&) = delete;
        // ������ֵ
        Logger& operator=(const Logger&) = delete;

        // �ƶ����캯����������־��¼������֮���ƶ�
        Logger(Logger&& rhs) :
            _name(std::move(rhs._name)), _attachedHandlers(std::move(rhs._attachedHandlers)) {
        }

        // log��ͨ����־����ӿ�
        // ��Ҫͨ��ģ�����ָ���������־�ȼ�
        // ͨ��requiresԼ����������־��¼���趨�ȼ�Ҫ�͵���־
        // ��������ʱͨ��if�ж�
        template <Level level>
            requires (level > loggerLevel)
        Logger& log(const std::string& message) {
            return *this;
        }

        // ͨ��requiresԼ���ύ�ȼ�Ϊ��־��¼���趨�ȼ������ϵ���־
        template <Level level>
            requires (level <= loggerLevel)
        Logger & log(const std::string& message) {
            // ����Record����
            Record record{
                .name = _name,
                .level = level,
                .time = std::chrono::system_clock::now(),
                .message = message,
            };

            // ����handleLogʵ�ʴ�����־���
            handleLog<level, HandlerCount - 1>(record);

            return *this;
        }

        // handleLog������־��¼�ύ������ע�����־������
        // messageLevelΪ�ύ����־�ȼ�
        // handlerIndexΪ��־��������ע�����
        // ͨ��requiresԼ����handlerIndex > 0ʱ��ݹ����handleLog����Ϣͬʱ�ύ��ǰһ����־������
        template <Level messageLevel, int32_t handlerIndex>
            requires (handlerIndex > 0)
        void handleLog(const Record& record) {
            // �ݹ����handleLog����Ϣͬʱ�ύ��ǰһ����־������
            handleLog<messageLevel, handlerIndex - 1>(record);

            // ��ȡ��ǰ��־���������ύ��Ϣ
            auto& handler = std::get<handlerIndex>(_attachedHandlers);
            handler.emit<messageLevel>(record);
        }

        template <Level messageLevel, int32_t handlerIndex>
            requires (handlerIndex == 0)
        void handleLog(const Record& record) {
            // ��ȡ��ǰ��־���������ύ��Ϣ
            auto& handler = std::get<handlerIndex>(_attachedHandlers);
            handler.emit<messageLevel>(record);
        }

        // �ύ���ش�����Ϣ��log�İ�װ��
        Logger& critical(const std::string& message) {
            return log<Level::Critical>(message);
        }

        // �ύһ�������Ϣ��log�İ�װ��
        Logger& error(const std::string& message) {
            return log<Level::Error>(message);
        }

        // �ύ������Ϣ��log�İ�װ��
        Logger& warning(const std::string& message) {
            return log<Level::Warning>(message);
        }

        // �ύ��ͨ��Ϣ��log�İ�װ��
        Logger& info(const std::string& message) {
            return log<Level::Info>(message);
        }

        // �ύ������Ϣ��log�İ�װ��
        Logger& debug(const std::string& message) {
            return log<Level::Debug>(message);
        }

        // �ύ���������Ϣ��log�İ�װ��
        Logger& trace(const std::string& message) {
            return log<Level::Trace>(message);
        }

    private:
        // ��־��¼������
        std::string _name;
        // ע�����־��������������־�������������������������������ʹ��Ԫ���������
        std::tuple<HandlerTypes...> _attachedHandlers;
    };

    // ��־��¼�����ɹ���
    template <Level level = Level::Warning>
    class LoggerFactory {
    public:
        // ������־��¼����ָ�������봦����
        template <Handler... HandlerTypes>
        static Logger<level, HandlerTypes...> createLogger(const std::string& name, std::tuple<HandlerTypes...>&& attachedHandlers) {
            return Logger<level, HandlerTypes...>(name, std::forward<std::tuple<HandlerTypes...>>(attachedHandlers));
        }

        // ������־��¼����ָ�����ƣ�����������Ĭ�ϴ�������DefaultHandler��
        template <Handler... HandlerTypes>
        static Logger<level, handlers::DefaultHandler<level>> createLogger(const std::string& name) {
            return Logger<level, handlers::DefaultHandler<level>>(name, std::make_tuple(handlers::DefaultHandler<level>()));
        }
    };
}