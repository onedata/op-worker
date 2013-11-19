/**
 * @file communicationHandler.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communicationHandler.h"
#include "glog/logging.h"
#include <iostream>
#include <string>

/// How many re-attampts has to be made by CommunicationHandler::communicate
/// before returning error.
#define RECONNECT_COUNT 1

/// Timout for WebSocket handshake
#define CONNECT_TIMEOUT 5000

/// Path on which cluster listenes for websocket connections
#define CLUSTER_URI_PATH "/veilclient"

using namespace std;
using namespace boost;
using namespace veil::protocol::communication_protocol;
using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;
using websocketpp::lib::bind;

namespace veil {

volatile int CommunicationHandler::instancesCount = 0;
boost::mutex CommunicationHandler::m_instanceMutex;

CommunicationHandler::CommunicationHandler(string hostname, int port, string certPath)
    : m_hostname(hostname),
      m_port(port),
      m_certPath(certPath),
      m_connectStatus(CLOSED),
      m_currentMsgId(0)
{
    m_endpoint.clear_access_channels(websocketpp::log::alevel::all);
    m_endpoint.clear_error_channels(websocketpp::log::elevel::all);
    
    boost::unique_lock<boost::mutex> lock(m_instanceMutex);
    ++instancesCount;
}
    
    
CommunicationHandler::~CommunicationHandler()
{
    closeConnection();
    boost::unique_lock<boost::mutex> lock(m_instanceMutex);
    --instancesCount;
}
    
int CommunicationHandler::openConnection()
{
    boost::unique_lock<boost::mutex> lock(m_connectMutex);
    websocketpp::lib::error_code ec;
        
    if(m_connectStatus == CONNECTED)
        return 0;
    
    // Initialize ASIO
    if(m_io_service) { // If ASIO io_service exists, then make sure that previous worker thread are stopped before we destroy that io_service
        m_io_service->stop();
        m_worker1.join();
        m_worker2.join();
    }
    
    // (re)Initialize io_service
    m_io_service.reset(new boost::asio::io_service());
    m_endpoint.init_asio(m_io_service.get());
        
    // Register our handlers
    m_endpoint.set_tls_init_handler(bind(&CommunicationHandler::onTLSInit, this, ::_1));
    m_endpoint.set_socket_init_handler(bind(&CommunicationHandler::onSocketInit, this, ::_1, ::_2));    // On socket init
    m_endpoint.set_message_handler(bind(&CommunicationHandler::onMessage, this, ::_1, ::_2));           // Incoming WebSocket message
    m_endpoint.set_open_handler(bind(&CommunicationHandler::onOpen, this, ::_1));                       // WebSocket connection estabilished
    m_endpoint.set_close_handler(bind(&CommunicationHandler::onClose, this, ::_1));                     // WebSocket connection closed
    m_endpoint.set_fail_handler(bind(&CommunicationHandler::onFail, this, ::_1));
    m_endpoint.set_ping_handler(bind(&CommunicationHandler::onPing, this, ::_1, ::_2));
    m_endpoint.set_pong_handler(bind(&CommunicationHandler::onPong, this, ::_1, ::_2));
    m_endpoint.set_pong_timeout_handler(bind(&CommunicationHandler::onPongTimeout, this, ::_1, ::_2));
    m_endpoint.set_interrupt_handler(bind(&CommunicationHandler::onInterrupt, this, ::_1));
        
        
    m_connectStatus = TIMEOUT;
    string URL = string("wss://") + m_hostname + ":" + toString(m_port) + string(CLUSTER_URI_PATH);
    ws_client::connection_ptr con = m_endpoint.get_connection(URL, ec); // Initialize WebSocket handshake
    if(ec.value() != 0) {
        LOG(ERROR) << "Cannot connect to " << URL << " due to: " << ec.message();
        return ec.value();
    } else {
        LOG(INFO) << "Trying to connect to: " << URL;
    }
        
    m_endpoint.connect(con);
    m_endpointConnection = con;
        
    // Start worker thread(s)
    // Second worker should not be started if WebSocket client lib cannot handle full-duplex connections
    m_worker1 = websocketpp::lib::thread(&ws_client::run, &m_endpoint);
    //m_worker2 = websocketpp::lib::thread(&ws_client::run, &m_endpoint);
    
    // Wait for WebSocket handshake
    m_connectCond.timed_wait(lock, boost::posix_time::milliseconds(CONNECT_TIMEOUT));
    
    LOG(INFO) << "Connection to " << URL << " status: " << m_connectStatus;
        
    return m_connectStatus;
}
    
    
void CommunicationHandler::closeConnection()
{
    boost::unique_lock<boost::mutex> lock(m_connectMutex);
    
    if(m_connectStatus == CLOSED)
        return;
    
    if(m_io_service) // Do not attempt to close connection if underlying io_service wasnt initialized
    {
        websocketpp::lib::error_code ec;
        m_endpoint.close(m_endpointConnection, websocketpp::close::status::normal, string("reset_by_peer"), ec); // Initialize WebSocket cloase operation
        if(ec.value() == 0)
            m_connectCond.timed_wait(lock, boost::posix_time::milliseconds(CONNECT_TIMEOUT)); // Wait for connection to close
        
        m_io_service->stop(); // If connection failed to close, make sure that io_service refuses to send/receive any further messages at this point
    }
    
    // Stop workers
    m_worker1.join();
    m_worker2.join();
    
    m_connectStatus = CLOSED;
}


int CommunicationHandler::sendMessage(const ClusterMsg& msg, long long msgId)
{
    if(m_connectStatus != CONNECTED)
        return m_connectStatus;
    
    websocketpp::lib::error_code ec;
    m_endpoint.send(m_endpointConnection, msg.SerializeAsString(), websocketpp::frame::opcode::binary, ec); // Initialize send operation (async)
        
    return ec.value();
}
    
long long CommunicationHandler::getMsgId()
{
    boost::unique_lock<boost::mutex> lock(m_msgIdMutex);
    return m_currentMsgId++;
}
    
int CommunicationHandler::receiveMessage(Answer& answer, long long msgId)
{
    boost::unique_lock<boost::mutex> lock(m_receiveMutex);
    
    // Incoming message should be in inbox. Wait for it
    while(m_incomingMessages.find(msgId) == m_incomingMessages.end())
        if(!m_receiveCond.timed_wait(lock, boost::posix_time::milliseconds(1000)))
            return -1;
        
    answer.ParseFromString(m_incomingMessages[msgId]);
    m_incomingMessages.erase(m_incomingMessages.find(msgId));
        
    return 0;
}
    
Answer CommunicationHandler::communicate(ClusterMsg& msg, uint8_t retry)
{
    Answer answer;

    if(sendMessage(msg, 0) < 0)
    {
        if(retry > 0) 
        {
            LOG(WARNING) << "Receiving response from cluster failed, trying to reconnect and retry";
            closeConnection();
            if(openConnection() == 0)
                return communicate(msg, retry - 1);
        }
            
        LOG(ERROR) << "TCP communication error";
        answer.set_answer_status(VEIO);

        return answer;
    }

    if(receiveMessage(answer, 0) < 0)
    {
        if(retry > 0) 
        {
            LOG(WARNING) << "Receiving response from cluster failed, trying to reconnect and retry";
            closeConnection();
            if(openConnection() == 0)
                return communicate(msg, retry - 1);
        }

        LOG(ERROR) << "TCP communication error";
        answer.set_answer_status(VEIO);
    }

    if(answer.answer_status() != VOK)
        LOG(INFO) << "Received answer with non-ok status: " << answer.answer_status();

    return answer;
}

int CommunicationHandler::getInstancesCount()
{
    boost::unique_lock<boost::mutex> lock(m_instanceMutex);
    return instancesCount;
}
    
context_ptr CommunicationHandler::onTLSInit(websocketpp::connection_hdl hdl)
{
    // Setup TLS connection (i.e. certificates)
    context_ptr ctx(new boost::asio::ssl::context(boost::asio::ssl::context::tlsv1));
        
    try {
        ctx->set_options(boost::asio::ssl::context::default_workarounds |
                         boost::asio::ssl::context::no_sslv2 |
                         boost::asio::ssl::context::single_dh_use);
            
        ctx->use_certificate_chain_file(m_certPath);
        ctx->use_private_key_file(m_certPath, boost::asio::ssl::context::pem);
    } catch (std::exception& e) {
        LOG(ERROR) << "Cannot initialize TLS socket due to:" << e.what();
    }
        
    return ctx;
}
    
void CommunicationHandler::onSocketInit(websocketpp::connection_hdl hdl,socket_type &socket)
{
    // Disable socket delay
    socket.lowest_layer().set_option(boost::asio::ip::tcp::no_delay(true));
}
    
void CommunicationHandler::onMessage(websocketpp::connection_hdl hdl, message_ptr msg)
{
    long long msgId = 0;
        
    boost::unique_lock<boost::mutex> lock(m_receiveMutex);
    m_incomingMessages[msgId] = msg->get_payload();         // Save incloming message to inbox and notify waiting threads
    m_receiveCond.notify_all();
}
    
void CommunicationHandler::onOpen(websocketpp::connection_hdl hdl)
{
    boost::unique_lock<boost::mutex> lock(m_connectMutex);
    m_connectStatus = CONNECTED;
    m_connectCond.notify_all();
}
    
void CommunicationHandler::onClose(websocketpp::connection_hdl hdl)
{
    boost::unique_lock<boost::mutex> lock(m_connectMutex);
    try {
        m_endpoint.get_con_from_hdl(hdl)->get_socket().lowest_layer().close();  // Explicite close underlying socket to make sure that all ongoing operations will be canceld
    } catch (boost::exception &e) {
        LOG(ERROR) << "WebSocket connection socket close error";
    }
    
    m_connectStatus = CLOSED;
    m_connectCond.notify_all();
}
    
void CommunicationHandler::onFail(websocketpp::connection_hdl hdl)
{
    boost::unique_lock<boost::mutex> lock(m_connectMutex);
    m_connectStatus = HANDSHAKE_ERROR;
    m_connectCond.notify_all();
    
    LOG(ERROR) << "WebSocket handshake error";
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
    boost::unique_lock<boost::mutex> lock(m_connectMutex);
    m_connectStatus = TRANSPORT_ERROR;
}

} // namespace veil