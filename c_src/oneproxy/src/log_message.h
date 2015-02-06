/**
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef LOG_MESSAGE_H
#define LOG_MESSAGE_H

#include <ostream>
#include <sstream>
#include <iostream>
#include <mutex>

#ifdef NDEBUG
#define LOG(SEVERITY)                                                          \
    one::proxy::log_message(one::proxy::log_message::SEVERITY, std::cout,      \
                            stdout_mutex, false)
#else
#define LOG(SEVERITY)                                                          \
    one::proxy::log_message(one::proxy::log_message::SEVERITY, std::cout,      \
                            stdout_mutex, true)
#endif

namespace one {
namespace proxy {

/// Mutex for synchronizing stdout in whole oneproxy (for erlang port driver)
extern std::mutex stdout_mutex;

/**
 * The log_message class.
 * Single instance handles single log message.
 */
class log_message {
public:
    /// Log severity
    enum severity { DEBUG, INFO, WARNING, ERROR };

    /**
     * Logs message to given @c ostream
     * @param s Log severity
     * @param sink Output ostream
     * @param debug_enabled Tells whether debug logs shall be logged
     */
    log_message(severity s, std::ostream &sink, std::mutex &sink_mutex,
                bool debug_enabled = false)
        : sink_(sink)
        , severity_(s)
        , debug_enabled_(debug_enabled)
        , stdout_lock(sink_mutex)
    {
        if (should_log())
            sink_ << severity_prefix() << " ";
    };

    /// Tells whether current log should be logged
    bool should_log()
    {
        return (severity_ == DEBUG && debug_enabled_) || severity_ != DEBUG;
    }

    /// Converts log severity to string log prefix
    std::string severity_prefix()
    {
        switch (severity_) {
            case DEBUG:
                return "[ DEBUG ]";
            case INFO:
                return "[ INFO ]";
            case WARNING:
                return "[ WARNING ]";
            case ERROR:
                return "[ ERROR ]";
            default:
                return std::to_string(severity_);
        }
    }

    /// Logs given object
    template <typename T> log_message &operator<<(const T &object)
    {
        if (should_log())
            sink_ << object;
        return *this;
    }

    /// Logs new line and flushes the sink
    virtual ~log_message()
    {
        if (should_log())
            sink_ << std::endl;
        sink_.flush();
    }

private:
    std::ostream &sink_;
    severity severity_;
    bool debug_enabled_;

    std::lock_guard<std::mutex> stdout_lock;
};

} // namespace proxy
} // namespace one

#endif // LOG_MESSAGE_H
