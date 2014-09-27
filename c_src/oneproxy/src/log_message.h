#ifndef log_message_h
#define log_message_h

#include <ostream>
#include <sstream>
#include <iostream>
#include <mutex>

#ifdef NDEBUG
#define LOG(SEVERITY)                                                          \
    one::proxy::log_message(one::proxy::log_message::SEVERITY, std::cout, false)
#else
#define LOG(SEVERITY)                                                          \
    one::proxy::log_message(one::proxy::log_message::SEVERITY, std::cout, true)
#endif

namespace one {
namespace proxy {

/// Mutex for synchronizing stdout in whole oneproxy (for erlang port driver)
extern std::mutex stdout_mutex;

/**
 * The log_message class.
 * Single instance handles signle log message.
 */
class log_message : public std::ostream {
public:

    /// Log severity
    enum Severity {
        DEBUG,
        INFO,
        WARNING,
        ERROR
    };

    /**
     * Logs message to given @c ostream
     * @param s Log severity
     * @param sink Output ostream
     * @param debug_enabled Tells whether debug logs shall be logged
     */
    log_message(Severity s, std::ostream &sink, bool debug_enabled)
        : sink_(sink)
        , severity_(s)
        , debug_enabled_(debug_enabled)
        , stdout_lock(stdout_mutex)
    {
        if (should_log())
            sink_ << severity() << " ";
    };

    /// Tells whether current log should be logged
    inline bool should_log()
    {
        return (severity_ == DEBUG && debug_enabled_) || severity_ != DEBUG;
    }

    /// Converts log severity to string log prefix
    inline std::string severity()
    {
        std::string severity_str;
        switch (severity_) {
            case DEBUG:
                severity_str = "[ DEBUG ]";
                break;
            case INFO:
                severity_str = "[ INFO ]";
                break;
            case WARNING:
                severity_str = "[ WARNING ]";
                break;
            case ERROR:
                severity_str = "[ ERROR ]";
                break;
            default:
                severity_str = std::to_string(severity_);
        }

        return severity_str;
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
    Severity severity_;
    bool debug_enabled_;

    std::lock_guard<std::mutex> stdout_lock;
};

} // namespace proxy
} // namespace one

#endif // log_message_h
