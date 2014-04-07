/**
 * @file logging.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "logging.h"

#include "communicationHandler.h"
#include "helpers/storageHelperFactory.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/chrono/duration.hpp>

#include <unistd.h>

#include <ctime>
#include <numeric>

static const boost::posix_time::seconds MAX_FLUSH_DELAY(10);
static const std::string CENTRAL_LOG_MODULE_NAME("central_logger");
static const std::string LOGGING_DECODER("logging");
static const std::queue<veil::protocol::logging::LogMessage>::size_type MAX_MESSAGE_BUFFER_SIZE = 1024;

namespace veil
{
namespace logging
{

boost::shared_ptr<RemoteLogWriter> logWriter(new RemoteLogWriter);
RemoteLogSink logSink(logWriter);
RemoteLogSink debugLogSink(logWriter, protocol::logging::LDEBUG);

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

RemoteLogWriter::RemoteLogWriter(const RemoteLogLevel initialThreshold)
    : m_pid(getpid())
    , m_thresholdLevel(initialThreshold)
{
    m_thread = boost::thread(&RemoteLogWriter::writeLoop, this);
}

RemoteLogWriter::~RemoteLogWriter()
{
    m_thread.interrupt(); // the thread will be interrupted at next buffer wait
    m_thread.join();
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

    pushMessage(log);
}

bool RemoteLogWriter::handleThresholdChange(const protocol::communication_protocol::Answer &answer)
{
    if(!boost::algorithm::iequals(answer.message_type(), "ChangeRemoteLogLevel"))
        return true;

    protocol::logging::ChangeRemoteLogLevel req;
    req.ParseFromString(answer.worker_answer());
    m_thresholdLevel = req.level();

    LOG(INFO) << "Client will now log " << req.level() <<
                 " and higher level messages to cluster.";

    return true;
}

void RemoteLogWriter::pushMessage(const protocol::logging::LogMessage &msg)
{
    boost::lock_guard<boost::mutex> guard(m_bufferMutex);
    if(m_buffer.size() > MAX_MESSAGE_BUFFER_SIZE)
        return; // silently drop message

    m_buffer.push(msg);
    m_bufferChanged.notify_all();
}

protocol::logging::LogMessage RemoteLogWriter::popMessage()
{
    boost::unique_lock<boost::mutex> lock(m_bufferMutex);
    while(m_buffer.empty())
        m_bufferChanged.timed_wait(lock, MAX_FLUSH_DELAY);

    const protocol::logging::LogMessage msg = m_buffer.front();
    m_buffer.pop();
    return msg;
}

void RemoteLogWriter::writeLoop()
{
    while(true)
    {
        const protocol::logging::LogMessage msg = popMessage();

        boost::shared_ptr<SimpleConnectionPool> connectionPool = helpers::config::getConnectionPool();
        if(!connectionPool)
            continue;

        boost::shared_ptr<CommunicationHandler> connection = connectionPool->selectConnection();
        if(!connection)
            continue;

        protocol::communication_protocol::ClusterMsg clm;
        clm.set_protocol_version(PROTOCOL_VERSION);
        clm.set_synch(false);
        clm.set_module_name(CENTRAL_LOG_MODULE_NAME);
        clm.set_message_decoder_name(LOGGING_DECODER);
        clm.set_message_type(boost::algorithm::to_lower_copy(msg.GetDescriptor()->name()));
        clm.set_input(msg.SerializeAsString());

        connection->sendMessage(clm, IGNORE_ANSWER_MSG_ID);
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
