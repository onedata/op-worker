/**
 * @file communicationHandler.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communicationHandler.h"
#include "fuse_messages.pb.h"
#include "logging.h"
#include "helpers/storageHelperFactory.h"
#include <google/protobuf/descriptor.h>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using std::string;
using namespace veil::protocol::communication_protocol;
using namespace veil::protocol::fuse_messages;
using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;
using websocketpp::lib::bind;

namespace veil {


CommunicationHandler::CommunicationHandler(const string &p_hostname, int p_port, cert_info_fun p_getCertInfo,const bool checkCertificate,
                                           boost::shared_ptr<ws_client> endpoint)
    : m_checkCertificate(checkCertificate),
      m_hostname(p_hostname),
      m_port(p_port),
      m_getCertInfo(p_getCertInfo),
      m_endpoint(std::move(endpoint)),
      m_connectStatus(CLOSED),
      m_nextMsgId(1),
      m_errorCount(0),
      m_isPushChannel(false),
      m_lastConnectTime(0)
{
}

void CommunicationHandler::setCertFun(cert_info_fun p_getCertInfo)
{
    m_getCertInfo = p_getCertInfo;
}


CommunicationHandler::~CommunicationHandler()
{
    DLOG(INFO) << "Destructing connection: " << this;
    closeConnection();
    DLOG(INFO) << "Connection: " << this << " deleted";
}

unsigned int CommunicationHandler::getErrorCount()
{
    return m_errorCount;
}

void CommunicationHandler::setFuseID(const std::string &fuseId)
{
    m_fuseID = fuseId;
}

void CommunicationHandler::setPushCallback(push_callback cb)
{
    unique_lock lock(m_connectMutex);
    m_pushCallback = cb; // Register callback
}

void CommunicationHandler::enablePushChannel()
{
    unique_lock lock(m_connectMutex);
    // If channel wasnt active and connection is esabilished atm, register channel
    // If connection is not active, let openConnection() take care of it
    if(!m_isPushChannel && m_connectStatus == CONNECTED && m_pushCallback)
        registerPushChannel(m_pushCallback);

    m_isPushChannel = true;
}

void CommunicationHandler::disablePushChannel()
{
    unique_lock lock(m_connectMutex);
    // If connection is not active theres no way to close PUSH channel
    if(m_isPushChannel && m_connectStatus == CONNECTED)
        closePushChannel();

    m_isPushChannel = false;
}

int CommunicationHandler::openConnection()
{
    unique_lock lock(m_connectMutex);
    websocketpp::lib::error_code ec;

    if(m_connectStatus == CONNECTED)
        return 0;

    m_connectStatus = TIMEOUT;
    m_endpointConnection.reset();

    string URL = string("wss://") + m_hostname + ":" + toString(m_port) + string(CLUSTER_URI_PATH);
    ws_client::connection_ptr con = m_endpoint->get_connection(URL, ec); // Initialize WebSocket handshake
    if(ec.value() != 0) {
        LOG(ERROR) << "Cannot connect to " << URL << " due to: " << ec.message();
        m_errorCount += MAX_CONNECTION_ERROR_COUNT + 1; // Force connection reinitialization
        return ec.value();
    } else {
        LOG(INFO) << "Trying to connect to: " << URL;
    }

    // Register our handlers
    con->set_message_handler(bind(&CommunicationHandler::onMessage, this, ::_1, ::_2));           // Incoming WebSocket message
    con->set_open_handler(bind(&CommunicationHandler::onOpen, this, ::_1));                       // WebSocket connection estabilished
    con->set_close_handler(bind(&CommunicationHandler::onClose, this, ::_1));                     // WebSocket connection closed
    con->set_fail_handler(bind(&CommunicationHandler::onFail, this, ::_1));
    con->set_ping_handler(bind(&CommunicationHandler::onPing, this, ::_1, ::_2));
    con->set_pong_handler(bind(&CommunicationHandler::onPong, this, ::_1, ::_2));
    con->set_pong_timeout_handler(bind(&CommunicationHandler::onPongTimeout, this, ::_1, ::_2));
    con->set_interrupt_handler(bind(&CommunicationHandler::onInterrupt, this, ::_1));

    m_endpoint->connect(con);
    m_endpointConnection = con;

    // Wait for WebSocket handshake
    m_connectCond.timed_wait(lock, boost::posix_time::milliseconds(CONNECT_TIMEOUT));

    LOG(INFO) << "Connection to " << URL << " status: " << m_connectStatus;

    if(m_connectStatus == 0 && !sendHandshakeACK())
        LOG(WARNING) << "Cannot set fuseId for the connection. Cluster will reject most of messages.";

    if(m_connectStatus == 0 && m_isPushChannel && m_pushCallback && m_fuseID.size() > 0)
        registerPushChannel(m_pushCallback);

    if(m_connectStatus == HANDSHAKE_ERROR) { // Force connection reinitialization on websocket handshake error
        m_errorCount += MAX_CONNECTION_ERROR_COUNT + 1;
    } else if(m_connectStatus < 0) {
        ++m_errorCount;
    }

    if(m_connectStatus == CONNECTED)
        m_lastConnectTime = helpers::utils::mtime<uint64_t>();

    return m_connectStatus;
}


void CommunicationHandler::closeConnection()
{
    unique_lock lock(m_connectMutex);

    if(m_connectStatus == CLOSED)
        return;

    if(m_endpoint) // Do not attempt to close connection if underlying io_service wasnt initialized
    {
        websocketpp::lib::error_code ec;
        m_endpoint->close(m_endpointConnection, websocketpp::close::status::normal, string("reset_by_peer"), ec); // Initialize WebSocket cloase operation
        if(ec.value() == 0)
            m_connectCond.timed_wait(lock, boost::posix_time::milliseconds(CONNECT_TIMEOUT)); // Wait for connection to close

        try {
            if(m_endpointConnection) {
                LOG(INFO) << "WebSocket: Lowest layer socket closed.";
                boost::system::error_code ec;
                m_endpointConnection->get_socket().lowest_layer().cancel(ec);  // Explicite close underlying socket to make sure that all ongoing operations will be canceld
                m_endpointConnection->get_socket().lowest_layer().close(ec);
            }
        } catch (boost::exception &e) {
            LOG(ERROR) << "WebSocket connection socket close error";
        }
    }

    m_connectStatus = CLOSED;
}


void CommunicationHandler::registerPushChannel(push_callback callback)
{
    LOG(INFO) << "Sending registerPushChannel request with FuseId: " << m_fuseID;

    m_pushCallback = callback; // Register callback
    m_isPushChannel = true;

    // Prepare PUSH channel registration request message
    ClusterMsg msg;
    ChannelRegistration reg;
    Atom at;
    reg.set_fuse_id(m_fuseID);

    msg.set_module_name("fslogic");
    msg.set_protocol_version(PROTOCOL_VERSION);
    msg.set_message_type(helpers::utils::tolower(reg.GetDescriptor()->name()));
    msg.set_message_decoder_name(helpers::utils::tolower(FUSE_MESSAGES));
    msg.set_answer_type(helpers::utils::tolower(Atom::descriptor()->name()));
    msg.set_answer_decoder_name(helpers::utils::tolower(COMMUNICATION_PROTOCOL));
    msg.set_synch(1);
    msg.set_input(reg.SerializeAsString());

    Answer ans = communicate(msg, 0);    // Send PUSH channel registration request
    at.ParseFromString(ans.worker_answer());

    LOG(INFO) << "PUSH channel registration status: " << ans.worker_answer() << ": " << at.value();
}

bool CommunicationHandler::sendHandshakeACK()
{
    ClusterMsg msg;
    HandshakeAck ack;

    LOG(INFO) << "Sending HandshakeAck with fuseId: '" << m_fuseID << "'";

    // Build HandshakeAck message
    ack.set_fuse_id(m_fuseID);

    msg.set_module_name("");
    msg.set_protocol_version(PROTOCOL_VERSION);
    msg.set_message_type(helpers::utils::tolower(ack.GetDescriptor()->name()));
    msg.set_message_decoder_name(helpers::utils::tolower(FUSE_MESSAGES));
    msg.set_answer_type(helpers::utils::tolower(Atom::descriptor()->name()));
    msg.set_answer_decoder_name(helpers::utils::tolower(COMMUNICATION_PROTOCOL));
    msg.set_synch(1);
    msg.set_input(ack.SerializeAsString());

    // Send HandshakeAck to cluster
    Answer ans = communicate(msg, 0);

    return ans.answer_status() == VOK;
}

void CommunicationHandler::closePushChannel()
{
    m_isPushChannel = false;

    // Prepare PUSH channel unregistration request message
    ClusterMsg msg;
    ChannelClose reg;
    reg.set_fuse_id(m_fuseID);

    msg.set_module_name("fslogic");
    msg.set_protocol_version(PROTOCOL_VERSION);
    msg.set_message_type(helpers::utils::tolower(reg.GetDescriptor()->name()));
    msg.set_message_decoder_name(helpers::utils::tolower(FUSE_MESSAGES));
    msg.set_answer_type(helpers::utils::tolower(Atom::descriptor()->name()));
    msg.set_answer_decoder_name(helpers::utils::tolower(COMMUNICATION_PROTOCOL));
    msg.set_synch(1);
    msg.set_input(reg.SerializeAsString());

    Answer ans = communicate(msg, 0);    // Send PUSH channel close request
}

int32_t CommunicationHandler::sendMessage(ClusterMsg& msg, MsgId msgId)
{
    if(m_connectStatus != CONNECTED)
        throw static_cast<ConnectionStatus>(m_connectStatus);

    // If message ID is not set, generate new one
    if (!msgId)
        msgId = getMsgId();

    msg.set_message_id(msgId);

    websocketpp::lib::error_code ec;
    m_endpoint->send(m_endpointConnection, msg.SerializeAsString(), websocketpp::frame::opcode::binary, ec); // Initialize send operation (async)

    if(ec.value())
        ++m_errorCount;

    if(ec.value() != 0) {
        LOG(ERROR) << "Cannot send message (ID: " << msgId << ") due to websocketpp's error: " << ec.message();
        throw UNDERLYING_LIB_ERROR;
    }

    return msgId;
}

int32_t CommunicationHandler::sendMessage(ClusterMsg& msg, MsgId msgId, ConnectionStatus &ec)
{
    try
    {
        ec = NO_ERROR;
        return sendMessage(msg, msgId);
    }
    catch(ConnectionStatus &e)
    {
        ec = e;
    }

    return 0;
}

MsgId CommunicationHandler::getMsgId()
{
    std::lock_guard<std::mutex> guard{m_msgIdMutex};
    m_nextMsgId = (m_nextMsgId % MAX_GENERATED_MSG_ID) + 1;
    return m_nextMsgId;
}

int CommunicationHandler::receiveMessage(Answer& answer, MsgId msgId, uint32_t timeout)
{
    unique_lock lock(m_receiveMutex);

    uint64_t timeoutTime = helpers::utils::mtime<uint64_t>() + timeout;

    // Incoming message should be in inbox. Wait for it
    while(m_incomingMessages.find(msgId) == m_incomingMessages.end()) {
        uint64_t currTime = helpers::utils::mtime<uint64_t>();

        if(m_connectStatus != CONNECTED) {
            ++m_errorCount;
            return -2;
        }

        if(currTime >= timeoutTime || !m_receiveCond.timed_wait(lock, boost::posix_time::milliseconds(timeoutTime - currTime))) {
            ++m_errorCount;
            return -1;
        }
    }

    answer.ParseFromString(m_incomingMessages[msgId]);
    m_incomingMessages.erase(m_incomingMessages.find(msgId));

    return 0;
}

Answer CommunicationHandler::communicate(ClusterMsg& msg, uint8_t retry, uint32_t timeout)
{
    Answer answer;
    if (timeout == 0)
    {
        timeout = msg.ByteSize() * 2; // 2ms for each byte (minimum of 500B/s)
    }

    if (timeout < RECV_TIMEOUT)   // Minimum timeout threshold
        timeout = RECV_TIMEOUT;

    try
    {
        unsigned int msgId = getMsgId();

        try
        {
            sendMessage(msg, msgId);
        }
        catch(ConnectionStatus)
        {
            if(retry > 0)
            {
                uint64_t lastConnectTime = m_lastConnectTime;

                LOG(WARNING) << "Sening message to cluster failed, trying to reconnect and retry";
                unique_lock guard(m_reconnectMutex);
                if(lastConnectTime != m_lastConnectTime) {
                    return communicate(msg, retry - 1);
                }

                LOG(INFO) << "Initializing reconnect sequence...";
                closeConnection();
                if(openConnection() == 0)
                    return communicate(msg, retry - 1);
            }

            LOG(ERROR) << "WebSocket communication error";
            DLOG(INFO) << "Error counter: " << m_errorCount;

            answer.set_answer_status(VEIO);

            return answer;
        }

        if(receiveMessage(answer, msgId, timeout) != 0)
        {
            if(retry > 0)
            {
                uint64_t lastConnectTime = m_lastConnectTime;

                LOG(WARNING) << "Receiving response from cluster failed, trying to reconnect and retry";
                unique_lock guard(m_reconnectMutex);
                if(lastConnectTime != m_lastConnectTime) {
                    return communicate(msg, retry - 1);
                }

                LOG(INFO) << "Initializing reconnect sequence...";
                closeConnection();
                if(openConnection() == 0)
                    return communicate(msg, retry - 1);
            }

            LOG(ERROR) << "WebSocket communication error";
            DLOG(INFO) << "Error counter: " << m_errorCount;

            answer.set_answer_status(VEIO);
        }

        if(answer.answer_status() != VOK)
        {
            LOG(INFO) << "Received answer with non-ok status: " << answer.answer_status();

            // Dispatch error to client if possible using PUSH channel
            if(m_pushCallback)
                m_pushCallback(answer);
        }


    } catch (websocketpp::lib::error_code &e) {
        LOG(ERROR) << "Unhandled WebSocket exception: " << e.message();
    }

    return answer;
}

void CommunicationHandler::onMessage(websocketpp::connection_hdl hdl, message_ptr msg)
{
    Answer answer;

    if(!answer.ParseFromString(msg->get_payload()))   // Ignore invalid answer
        return;

    if(answer.message_id() == IGNORE_ANSWER_MSG_ID)   // Ignore ignored answer
        return;

    if(answer.message_id() < 0) // PUSH message
    {
        if(m_pushCallback)
            m_pushCallback(answer); // Dispatch PUSH message to registered callback
        else
            LOG(WARNING) << "Received PUSH message (ID: " << answer.message_id() <<") but the channel is not registered as PUSH listener. Ignoring.";

        return;
    }

    // Continue normally for non-PUSH message
    unique_lock lock(m_receiveMutex);
    m_incomingMessages[answer.message_id()] = msg->get_payload();         // Save incloming message to inbox and notify waiting threads
    m_receiveCond.notify_all();
}

void CommunicationHandler::onOpen(websocketpp::connection_hdl hdl)
{
    unique_lock lock(m_connectMutex);
    m_connectStatus = CONNECTED;
    LOG(INFO) << "WebSocket connection esabilished successfully.";
    m_connectCond.notify_all();
}

void CommunicationHandler::onClose(websocketpp::connection_hdl hdl)
{
    unique_lock lock(m_connectMutex);
    ++m_errorCount; // Closing connection means that something went wrong

    m_connectStatus = CLOSED;
    m_connectCond.notify_all();
    m_receiveCond.notify_all();
}

void CommunicationHandler::onFail(websocketpp::connection_hdl hdl)
{
    unique_lock lock(m_connectMutex);

    ++m_errorCount;

    m_connectStatus = HANDSHAKE_ERROR;
    m_connectCond.notify_all();
    m_receiveCond.notify_all();

    const ws_client::connection_ptr conn = m_endpoint->get_con_from_hdl(hdl);
    if(conn)
    {
        const int verifyResult =
            SSL_get_verify_result(conn->get_socket().native_handle());

        if(verifyResult != 0)
        {
            LOG(ERROR) << "Server certificate verification failed." <<
                          " OpenSSL error " << verifyResult;
            m_lastError.store(error::SERVER_CERT_VERIFICATION_FAILED);
        }
    }

    LOG(ERROR) << "WebSocket handshake error.";
}

bool CommunicationHandler::onPing(websocketpp::connection_hdl hdl, std::string msg)
{
    // No need to implement this
    return true;
}

void CommunicationHandler::onPong(websocketpp::connection_hdl hdl, std::string msg)
{
    // Since we dont ping cluster, this callback wont be called
}

void CommunicationHandler::onPongTimeout(websocketpp::connection_hdl hdl, std::string msg)
{
    // Since we dont ping cluster, this callback wont be called
    LOG(WARNING) << "WebSocket pong-message (" << msg << ") timed out";
}

void CommunicationHandler::onInterrupt(websocketpp::connection_hdl hdl)
{
    LOG(WARNING) << "WebSocket connection was interrupted";
    unique_lock lock(m_connectMutex);
    m_connectStatus = TRANSPORT_ERROR;
}

error::Error CommunicationHandler::getLastError() const
{
    return m_lastError.load();
}

} // namespace veil
