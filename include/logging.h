/**
 * @file logging.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_LOGGING_H
#define VEILHELPERS_LOGGING_H


#include "communication_protocol.pb.h"
#include "logging.pb.h"

#include <glog/logging.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>


#ifndef NDEBUG
#   undef DLOG
#   define DLOG(severity) LOG_TO_SINK(veil::logging::_debugLogSink.lock().get(), severity)
#   define DLOG_TO_SINK(sink, severity) LOG_TO_SINK(sink, severity)
#else
#   define DLOG_TO_SINK(sink, severity) \
        true ? (void) 0 : google::LogMessageVoidify() & LOG_TO_SINK(sink, severity)
#endif

#undef LOG
#define LOG(severity) LOG_TO_SINK(veil::logging::_logSink.lock().get(), severity)

namespace veil
{

class SimpleConnectionPool;

namespace logging
{

/**
 * An enum describing log levels. Every message of threshold level and higher
 * are logged to a cluster. Log levels are compared by their int values.
 */
typedef protocol::logging::LogLevel RemoteLogLevel;

class RemoteLogWriter;
class RemoteLogSink;

extern std::weak_ptr<RemoteLogSink> _logSink;
extern std::weak_ptr<RemoteLogSink> _debugLogSink;

/**
 * Sets RemoteLogSink objects used for logging normal and debug messages.
 * The setLogSinks function takes ownership of the pointers.
 * @param logSink The new log sink.
 * @param debugLogSink The new debug log sink.
 */
extern void setLogSinks(const std::shared_ptr<RemoteLogSink> &logSink, const std::shared_ptr<RemoteLogSink> &debugLogSink);

/**
 * The RemoteLogWriter class is responsible for sending log messages to a
 * cluster.
 */
class RemoteLogWriter
{
    using BufferSize = std::queue<protocol::logging::LogMessage>::size_type;
    static constexpr BufferSize DEFAULT_MAX_MESSAGE_BUFFER_SIZE = 1024;
    static constexpr BufferSize DEFAULT_MESSAGE_BUFFER_TRIM_SIZE = 850;

public:
    /**
     * Constructor. Sets the PID value sent with log messages to getpid().
     * @param initialThreshold The initial threshold level below which messages
     * won't be sent to a cluster.
     * @param maxBufferSize The maximum size of the buffer. When the buffer size
     * exceeds the maximum size, it is trimmed to @p bufferTrimSize and a
     * warning message is added to the buffer.
     * @param bufferTrimSize The size to which the buffer will be trimmed after
     * exceeding @p maxBufferSize .
     */
    RemoteLogWriter(const RemoteLogLevel initialThreshold = protocol::logging::NONE,
                    const BufferSize maxBufferSize = DEFAULT_MAX_MESSAGE_BUFFER_SIZE,
                    const BufferSize bufferTrimSize = DEFAULT_MESSAGE_BUFFER_TRIM_SIZE);

    /**
     * Runs the message write loop in a separate thread.
     * Sets the connection pool used by the writer to send logs to a cluster.
     * @param connectionPool The pool to be used by the writer.
     */
    virtual void run(std::shared_ptr<SimpleConnectionPool> connectionPool);

    /**
     * Destructor.
     */
    virtual ~RemoteLogWriter();

    /**
     * Saves a message to be sent to a cluster in a buffer. The buffer is
     * read by a separate thread and the queued messages are sent by it.
     * @param level Severity level of the log message.
     * @param fileName The name of the file in which the message was logged.
     * @param line The line of the file in which the message was logged.
     * @param timestamp Time of logging the message.
     * @param message The message that was logged.
     */
    virtual void buffer(const RemoteLogLevel level, const std::string &fileName,
                        const int line, const time_t timestamp,
                        const std::string &message);

    /**
     * If the @p answer contains a ChangeRemoteLevel request from a cluster,
     * changes the minimum severiti level of messages sent to the cluster.
     * @param answer A push message from the cluster.
     * @return true.
     */
    virtual bool handleThresholdChange(const protocol::communication_protocol::Answer &answer);

private:
    void pushMessage(const protocol::logging::LogMessage &msg);
    protocol::logging::LogMessage popMessage();
    void writeLoop();
    void dropExcessMessages();

    std::shared_ptr<SimpleConnectionPool> m_connectionPool;
    const pid_t m_pid;
    const BufferSize m_maxBufferSize;
    const BufferSize m_bufferTrimSize;
    std::condition_variable m_bufferChanged;
    std::mutex m_bufferMutex;
    std::thread m_thread;
    std::atomic<RemoteLogLevel> m_thresholdLevel;
    std::atomic<bool> m_stopWriteLoop;
    std::queue<protocol::logging::LogMessage> m_buffer;
};

/**
 * A subclass of glog's LogSink, saves log messages to an instance of
 * RemoteLogWriter.
 */
class RemoteLogSink: public google::LogSink
{
public:
    /**
     * Constructor.
     * @param writer An instance of RemoteLogWriter which will consume messages.
     * @param forcedLevel A log message severity level to report to the writer.
     * If set to protocol::logging::NONE, the messages are reported with their
     * original severity level.
     */
    RemoteLogSink(std::shared_ptr<RemoteLogWriter> writer,
                  const RemoteLogLevel forcedLevel = protocol::logging::NONE);

    /**
     * Queues the message to an instance of RemoteLogWriter. Called by glog.
     * @see google::LogSink
     */
    void send(google::LogSeverity severity, const char* full_filename,
              const char* base_filename, int line,
              const struct ::tm* tm_time,
              const char* message, size_t message_len);

private:
    const RemoteLogLevel m_forcedLevel;
    std::shared_ptr<RemoteLogWriter> m_writer;
};

}
}


#endif // VEILHELPERS_LOGGING_H
