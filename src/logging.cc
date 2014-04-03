/**
 * @file logging.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "logging.h"

//#include "storageHelperFactory.h"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/chrono/duration.hpp>

#include <unistd.h>

#include <ctime>


namespace veil
{
namespace logging
{

boost::shared_ptr<RemoteLogWriter> logWriter(new RemoteLogWriter);
RemoteLogSink logSink(logWriter);
RemoteLogSink debugLogSink(logWriter, protocol::logging::LDEBUG);

const boost::posix_time::seconds MAX_FLUSH_DELAY(10);

static RemoteLogLevel glogToLevel(google::LogSeverity glevel)
{
    switch(glevel)
    {
        case google::INFO: return protocol::logging::INFO;
        case google::WARNING: return protocol::logging::WARNING;
        case google::ERROR: return protocol::logging::ERROR;
        case google::FATAL: return protocol::logging::FATAL;
        default: return protocol::logging::NONE;
    }
}

static std::string levelToStr(RemoteLogLevel level)
{
    switch(level)
    {
        case protocol::logging::LDEBUG: return "DEBUG";
        case protocol::logging::INFO: return "INFO";
        case protocol::logging::WARNING: return "WARNING";
        case protocol::logging::ERROR: return "ERROR";
        case protocol::logging::FATAL: return "FATAL";
        default: return "NONE";
    }
}

RemoteLogWriter::RemoteLogWriter()
    : m_pid(getpid())
{
    m_thread = boost::thread(&RemoteLogWriter::writeLoop, this);
    m_thresholdLevel = protocol::logging::NONE;
}

void RemoteLogWriter::buffer(const RemoteLogLevel level,
                             const std::string &fileName, const int line,
                             const time_t timestamp, const std::string &message)
{
    if(m_thresholdLevel > level) // nothing to log
        return;

    protocol::logging::LogMessage log;
    log.set_level(level);
    log.set_pid(m_pid);
    log.set_file_name(fileName);
    log.set_line(line),
    log.set_timestamp(timestamp);
    log.set_message(message);

    boost::lock_guard<boost::mutex> guard(m_bufferMutex);
    m_buffer.push(log);
    m_bufferChanged.notify_all();
}

bool RemoteLogWriter::handleThresholdChange(const protocol::communication_protocol::Answer &answer)
{
    if(!boost::algorithm::iequals(answer.message_type(), "ChangeRemoteLogLevel"))
        return true;

    protocol::logging::ChangeRemoteLogLevel req;
    req.ParseFromString(answer.worker_answer());
    m_thresholdLevel = req.level();

    LOG(INFO) << "Client will now log " << levelToStr(req.level()) <<
                 " and higher level messages to cluster.";

    return true;
}

protocol::logging::LogMessage RemoteLogWriter::popBuffer()
{
    boost::unique_lock<boost::mutex> lock(m_bufferMutex);
    while(m_buffer.empty())
        m_bufferChanged.timed_wait(lock, MAX_FLUSH_DELAY);

    const protocol::logging::LogMessage msg = m_buffer.front();
    m_buffer.pop();
    return msg;
}
#include <iostream>
void RemoteLogWriter::writeLoop()
{
    while(true)
    {
        const protocol::logging::LogMessage msg = popBuffer();
        if(msg.level() != protocol::logging::LDEBUG)
            DLOG(INFO) << "OMG";
        std::cout << "Sending a message [" << levelToStr(msg.level()) << "] " << msg.message() << std::endl;
    }
}

RemoteLogSink::RemoteLogSink(const boost::shared_ptr<RemoteLogWriter> &writer,
                             const RemoteLogLevel forcedLevel)
    : m_forcedLevel(forcedLevel)
    , m_writer(writer)
{
}

void RemoteLogSink::send(google::LogSeverity severity,
                         const char */*full_filename*/,
                         const char *base_filename, int line, const tm *tm_time,
                         const char *message, size_t message_len)
{
    const time_t timestamp = std::mktime(const_cast<tm*>(tm_time));
    const RemoteLogLevel level = m_forcedLevel != protocol::logging::NONE
            ? m_forcedLevel : glogToLevel(severity);

    m_writer->buffer(level, base_filename, line, timestamp,
                     std::string(message, message_len));
}

} // namespace logging
} // namespace veil

//void RemoteLoggerProxy::Write(bool force_flush, time_t timestamp,
//                              const char *message, int message_len)
//{
//    m_logger->Write(force_flush, timestamp, message, message_len);

//    if(s_remoteLevel == m_level)
//    {
//        auto connection = veil::helpers::config::getConnectionPool()->selectConnection();
//        if(!connection)
//            return;

////        auto config = VeilFS::getConfig();
////        if(!config)
////            return false;

//        LogMessage log;
////        log.set_fuse_id(config->getFuseID());
//        log.set_level(m_level);
//        log.set_timestamp(timestamp);
//        log.set_message(message);

//        ClusterMsg msg;
//        connection->sendMessage(msg);
//    }
//}
