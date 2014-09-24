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

    extern std::mutex stdout_mutex;

    class log_message : public std::ostream {
    public:
        enum Severity {
            DEBUG,
            INFO,
            WARNING,
            ERROR
        };

        log_message(Severity s, std::ostream &sink, bool debug_enabled)
            : sink_(sink)
            , severity_(s)
            , debug_enabled_(debug_enabled)
            , stdout_lock(stdout_mutex)
        {
            if (should_log())
                sink_ << severity() << " ";
        };

        inline bool should_log()
        {
            return (severity_ == DEBUG && debug_enabled_) || severity_ != DEBUG;
        }

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

        template <typename T> log_message &operator<<(const T &object)
        {
            if (should_log())
                sink_ << object;
            return *this;
        }

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