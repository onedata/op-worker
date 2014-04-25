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
#include <openssl/evp.h>
#include <openssl/err.h>

using std::string;
using namespace veil::protocol::communication_protocol;
using namespace veil::protocol::fuse_messages;
using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;
using websocketpp::lib::bind;

namespace veil {

boost::recursive_mutex CommunicationHandler::m_instanceMutex;
SSL_SESSION* CommunicationHandler::m_session = 0;

CommunicationHandler::CommunicationHandler(string p_hostname, int p_port, cert_info_fun p_getCertInfo)
    : m_hostname(p_hostname),
      m_port(p_port),
      m_getCertInfo(p_getCertInfo),
      m_connectStatus(CLOSED),
      m_currentMsgId(1),
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
    closeConnection();

    DLOG_TO_SINK(NULL, INFO) << "Destructing connection: " << this;
    if(m_endpoint)
    {
        m_endpoint->stop();
        m_endpoint.reset();
    }

    // Force exit worker threads
    if(m_worker1.joinable() && !m_worker1.timed_join(boost::posix_time::milliseconds(200)) && m_worker1.native_handle()) {
        pthread_cancel(m_worker1.native_handle());
    }

    if(m_worker2.joinable() && !m_worker2.timed_join(boost::posix_time::milliseconds(200)) && m_worker2.native_handle()) {
        pthread_cancel(m_worker2.native_handle());
    }

    DLOG_TO_SINK(NULL, INFO) << "Connection: " << this << " deleted";
}

unsigned int CommunicationHandler::getErrorCount()
{
    return m_errorCount;
}

void CommunicationHandler::setFuseID(string fuseId)
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

    // Initialize ASIO
    if(m_endpoint) { // If endpoint exists, then make sure that previous worker thread are stopped before we destroy that io_service
        m_endpoint->stop();
    }

    // (re)Initialize endpoint (io_service)
    m_endpointConnection.reset();
    m_endpoint.reset(new ws_client());
    m_endpoint->clear_access_channels(websocketpp::log::alevel::all);
    m_endpoint->clear_error_channels(websocketpp::log::elevel::all);
    m_endpoint->init_asio(ec);

    if(ec)
    {
        LOG(ERROR) << "Cannot initlize WebSocket endpoint";
        return m_connectStatus;
    }

    // Register our handlers
    m_endpoint->set_tls_init_handler(bind(&CommunicationHandler::onTLSInit, this, ::_1));
    m_endpoint->set_socket_init_handler(bind(&CommunicationHandler::onSocketInit, this, ::_1, ::_2));    // On socket init
    m_endpoint->set_message_handler(bind(&CommunicationHandler::onMessage, this, ::_1, ::_2));           // Incoming WebSocket message
    m_endpoint->set_open_handler(bind(&CommunicationHandler::onOpen, this, ::_1));                       // WebSocket connection estabilished
    m_endpoint->set_close_handler(bind(&CommunicationHandler::onClose, this, ::_1));                     // WebSocket connection closed
    m_endpoint->set_fail_handler(bind(&CommunicationHandler::onFail, this, ::_1));
    m_endpoint->set_ping_handler(bind(&CommunicationHandler::onPing, this, ::_1, ::_2));
    m_endpoint->set_pong_handler(bind(&CommunicationHandler::onPong, this, ::_1, ::_2));
    m_endpoint->set_pong_timeout_handler(bind(&CommunicationHandler::onPongTimeout, this, ::_1, ::_2));
    m_endpoint->set_interrupt_handler(bind(&CommunicationHandler::onInterrupt, this, ::_1));

    string URL = string("wss://") + m_hostname + ":" + toString(m_port) + string(CLUSTER_URI_PATH);
    ws_client::connection_ptr con = m_endpoint->get_connection(URL, ec); // Initialize WebSocket handshake
    if(ec.value() != 0) {
        LOG(ERROR) << "Cannot connect to " << URL << " due to: " << ec.message();
        m_errorCount += MAX_CONNECTION_ERROR_COUNT + 1; // Force connection reinitialization
        return ec.value();
    } else {
        LOG(INFO) << "Trying to connect to: " << URL;
    }

    {   unique_lock lock(m_instanceMutex);

        if(m_session) {
            socket_type &socket = con->get_socket();
            SSL *ssl = socket.native_handle();
            if(SSL_set_session(ssl, m_session) != 1) {
                LOG(ERROR) << "Cannot set session.";
            }
        }
    }

    m_endpoint->connect(con);
    m_endpointConnection = con;

    // Start worker thread(s)
    // Second worker should not be started if WebSocket client lib cannot handle full-duplex connections
    m_worker1 = boost::thread(&ws_client::run, m_endpoint);
    //m_worker2 = boost::thread(&ws_client::run, m_endpoint);

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

    if(m_connectStatus == CONNECTED) {
        m_lastConnectTime = helpers::utils::mtime<uint64_t>();
        unique_lock lock(m_instanceMutex);
        socket_type &socket = m_endpointConnection->get_socket();
        SSL *ssl = socket.native_handle();
        if(m_session) {
            SSL_SESSION_free(m_session);
        }
        m_session = SSL_get1_session(ssl);
    }

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

        m_endpoint->stop(); // If connection failed to close, make sure that io_service refuses to send/receive any further messages at this point

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

    // Stop workers
    m_worker1.timed_join(boost::posix_time::milliseconds(200));
    m_worker2.timed_join(boost::posix_time::milliseconds(200));

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

int32_t CommunicationHandler::sendMessage(ClusterMsg& msg, int32_t msgId)
{
    if(m_connectStatus != CONNECTED)
        throw m_connectStatus;

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

int32_t CommunicationHandler::sendMessage(ClusterMsg& msg, int32_t msgId, ConnectionStatus &ec) throw()
{
    try {
        return sendMessage(msg, msgId);
    } catch(ConnectionStatus &e) {
        ec = e;
    }

    return 0;
}

int32_t CommunicationHandler::getMsgId()
{
    unique_lock lock(m_msgIdMutex);
    m_currentMsgId = (m_currentMsgId % MAX_GENERATED_MSG_ID) + 1;
    return m_currentMsgId;
}

int CommunicationHandler::receiveMessage(Answer& answer, int32_t msgId, uint32_t timeout)
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

        ConnectionStatus ec;
        sendMessage(msg, msgId, ec);
        if(ec != NO_ERROR)
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

context_ptr CommunicationHandler::onTLSInit(websocketpp::connection_hdl hdl)
{
    if (!m_getCertInfo) {
        LOG(ERROR) << "Cannot get CertificateInfo due to null getter";
        return context_ptr();
    }

    CertificateInfo certInfo = m_getCertInfo();

    try {
        context_ptr ctx(new boost::asio::ssl::context(boost::asio::ssl::context::sslv3));

        ctx->set_options(boost::asio::ssl::context::default_workarounds |
                         boost::asio::ssl::context::no_sslv2 |
                         boost::asio::ssl::context::single_dh_use);

        ctx->set_default_verify_paths();
        ctx->set_verify_mode(helpers::config::checkCertificate.load()
                             ? boost::asio::ssl::verify_peer
                             : boost::asio::ssl::verify_none);

        SSL_CTX *ssl_ctx = ctx->native_handle();
        long mode = SSL_CTX_get_session_cache_mode(ssl_ctx);
        mode |= SSL_SESS_CACHE_CLIENT;
        SSL_CTX_set_session_cache_mode(ssl_ctx, mode);

        boost::asio::ssl::context_base::file_format file_format; // Certificate format
        if(certInfo.cert_type == CertificateInfo::ASN1) {
            file_format = boost::asio::ssl::context::asn1;
        } else {
            file_format = boost::asio::ssl::context::pem;
        }

        if(certInfo.cert_type == CertificateInfo::P12) {
            LOG(ERROR) << "Unsupported certificate format: P12";
            return context_ptr();
        }

        if(boost::asio::buffer_size(certInfo.chain_data) && boost::asio::buffer_size(certInfo.chain_data)) {
            ctx->use_certificate_chain(certInfo.chain_data);
            ctx->use_private_key(certInfo.key_data, file_format);
        } else {
            ctx->use_certificate_chain_file(certInfo.user_cert_path);
            ctx->use_private_key_file(certInfo.user_key_path, file_format);
        }

        return ctx;

    } catch (boost::system::system_error& e) {
        LOG(ERROR) << "Cannot initialize TLS socket due to: " << e.what()
                   << " with cert file: " << certInfo.user_cert_path << " and key file: "
                   << certInfo.user_key_path;
    }

    return context_ptr();
}

void CommunicationHandler::onSocketInit(websocketpp::connection_hdl hdl,socket_type &socket)
{
    // Disable socket delay
    socket.lowest_layer().set_option(boost::asio::ip::tcp::no_delay(true));
}

void CommunicationHandler::onMessage(websocketpp::connection_hdl hdl, message_ptr msg)
{
    Answer answer;

    if(!answer.ParseFromString(msg->get_payload()))   // Ignore invalid answer
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
