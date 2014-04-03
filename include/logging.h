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

#include <boost/atomic.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include <glog/logging.h>

#include <queue>
#include <string>


#if !defined(NDEBUG) && defined(DLOG)
#   undef DLOG
#   define DLOG(severity) LOG_TO_SINK(&veil::logging::debugLogSink, severity)
#endif

#if defined(LOG)
#   undef LOG
#   define LOG(severity) LOG_TO_SINK(&veil::logging::logSink, severity)
#endif

namespace veil
{
namespace logging
{

/**
 * An enum describing log levels. Every message of threshold level and higher
 * are logged to a cluster. Log levels are compared by their int values.
 */
typedef protocol::logging::LogLevel RemoteLogLevel;

class RemoteLogWriter;
class RemoteLogSink;

/**
 * A writer used by logSink and debugLogSink. Useful to subscribe
 * RemoteLogWriter::handleThresholdChange to a push listener.
 * @see veil::logging::logSink
 * @see veil::logging::debugLogSink
 */
extern boost::shared_ptr<RemoteLogWriter> logWriter;

/**
 * A sink for use with the LOG macro.
 */
extern RemoteLogSink logSink;

/**
 * A sink for use with the DLOG macro. This sink has forced message severity of
 * RemoteLogLevel::LDEBUG.
 */
extern RemoteLogSink debugLogSink;

/**
 * The RemoteLogWriter class is responsible for sending log messages to a
 * cluster.
 */
class RemoteLogWriter
{
public:
    /**
     * Constructor. Sets the PID value sent with log messages to getpid().
     */
    RemoteLogWriter();

    /**
     * Saves a message to be sent to a cluster in a buffer. The buffer is
     * read by a separate thread and the queued messages are sent by it.
     * @param level Severity level of the log message.
     * @param fileName The name of the file in which the message was logged.
     * @param line The line of the file in which the message was logged.
     * @param timestamp Time of logging the message.
     * @param message The message that was logged.
     */
    void buffer(const RemoteLogLevel level, const std::string &fileName,
                const int line, const time_t timestamp, const std::string &message);

    /**
     * If the @p answer contains a ChangeRemoteLevel request from a cluster,
     * changes the minimum severiti level of messages sent to the cluster.
     * @param answer A push message from the cluster.
     * @return true.
     */
    bool handleThresholdChange(const protocol::communication_protocol::Answer &answer);

private:
    void pushMessage(const protocol::logging::LogMessage &msg);
    protocol::logging::LogMessage popMessage();
    void writeLoop();

    const pid_t m_pid;
    boost::condition_variable m_bufferChanged;
    boost::mutex m_bufferMutex;
    boost::thread m_thread;
    boost::atomic<RemoteLogLevel> m_thresholdLevel;
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
    RemoteLogSink(const boost::shared_ptr<RemoteLogWriter> &writer,
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
    boost::shared_ptr<RemoteLogWriter> m_writer;
};

}
}

#endif // VEILHELPERS_LOGGING_H
