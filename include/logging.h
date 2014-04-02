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

typedef protocol::logging::LogLevel RemoteLogLevel;

class RemoteLogWriter;
class RemoteLogSink;
extern boost::shared_ptr<RemoteLogWriter> logWriter;
extern RemoteLogSink logSink;
extern RemoteLogSink debugLogSink;

class RemoteLogWriter
{
public:
    RemoteLogWriter();
    void buffer(const RemoteLogLevel level, const std::string &fileName,
                const int line, const time_t timestamp, const std::string &message);

    bool handleThresholdChange(const protocol::communication_protocol::Answer &answer);

private:
    protocol::logging::LogMessage popBuffer();
    void writeLoop();

    const int m_pid;
    boost::condition_variable m_bufferChanged;
    boost::mutex m_bufferMutex;
    boost::thread m_thread;
    boost::atomic<RemoteLogLevel> m_thresholdLevel;
    std::queue<protocol::logging::LogMessage> m_buffer;
};

class RemoteLogSink: public google::LogSink
{
public:
    RemoteLogSink(const boost::shared_ptr<RemoteLogWriter> &writer,
                  const RemoteLogLevel forcedLevel = protocol::logging::NONE);

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
