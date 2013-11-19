/**
 * @file communicationHandler.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */


#ifndef COMMUNICATION_HANDLER_H
#define COMMUNICATION_HANDLER_H

#include <string>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <websocketpp/config/asio_client.hpp>
#include <websocketpp/client.hpp>
#include "communication_protocol.pb.h"
#include "veilErrors.h"

typedef websocketpp::client<websocketpp::config::asio_tls_client>       ws_client;
typedef websocketpp::config::asio_tls_client::message_type::ptr         message_ptr;
typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket>          socket_type;

typedef websocketpp::lib::shared_ptr<boost::asio::ssl::context>         context_ptr;

template<typename T>
std::string toString(T in) {
    std::ostringstream ss;
    ss << in;
    return ss.str();
}

namespace veil {

/**
 * The CommunicationHandler class.
 * Object of this class represents WebSocket based connection to cluster.
 * The CommunicationHandler allows to communicate with cluster by sending and receiving
 * ClusterMsg messages. 
 */
class CommunicationHandler
{

protected:
    
    /// Current connection status codes
    enum ConnectionStatus
    {
        TIMEOUT             = -4,
        HANDSHAKE_ERROR     = -3,
        TRANSPORT_ERROR     = -2,
        CLOSED              = -1,
        CONNECTED           = 0
    };
    
    std::string                 m_hostname;
    int                         m_port;
    std::string                 m_certPath;
    
    // Container that gathers all incoming messages
    boost::unordered_map<long long, std::string> m_incomingMessages;
    
    // Boost based internals
    boost::shared_ptr<boost::asio::io_service>    m_io_service;
    ws_client                   m_endpoint;
    ws_client::connection_ptr   m_endpointConnection;
    boost::thread               m_worker1;
    boost::thread               m_worker2;
    volatile int                m_connectStatus;
    volatile unsigned long long m_currentMsgId;
    static volatile int         instancesCount;
    
    boost::mutex                m_connectMutex;
    boost::condition_variable   m_connectCond;
    boost::mutex                m_msgIdMutex;
    boost::mutex                m_receiveMutex;
    boost::condition_variable   m_receiveCond;
    static boost::mutex         m_instanceMutex;
    
    // WebSocket++ callbacks
    context_ptr onTLSInit(websocketpp::connection_hdl hdl);                 ///< On TLS init callback
    void onSocketInit(websocketpp::connection_hdl hdl, socket_type &socket);///< On socket init callback
    void onMessage(websocketpp::connection_hdl hdl, message_ptr msg);       ///< Incoming WebSocket message callback
    void onOpen(websocketpp::connection_hdl hdl);                           ///< WebSocket connection opened
    void onClose(websocketpp::connection_hdl hdl);                          ///< WebSocket connection closed
    void onFail(websocketpp::connection_hdl hdl);                           ///< WebSocket connection failed callback. This can proc only before CommunicationHandler::onOpen
    bool onPing(websocketpp::connection_hdl hdl, std::string);              ///< Ping received callback
    void onPong(websocketpp::connection_hdl hdl, std::string);              ///< Pong received callback
    void onPongTimeout(websocketpp::connection_hdl hdl, std::string);       ///< Cluaster failed to respond on ping message
    void onInterrupt(websocketpp::connection_hdl hdl);                      ///< WebSocket connection was interuped

public:
    CommunicationHandler(std::string hostname, int port, std::string certPath);
    virtual ~CommunicationHandler();

    virtual long long   getMsgId();                                         ///< Get next message id. Thread safe. All subsequents calls returns next integer value.
    virtual int         openConnection();                                   ///< Opens WebSoscket connection. Returns 0 on success, non-zero otherwise.
    virtual void        closeConnection();                                  ///< Closes active connection.
    virtual int         sendMessage(const protocol::communication_protocol::ClusterMsg& message, long long int msgID);             ///< Sends ClusterMsg using current WebSocket session. Will fail if there isnt one.
    virtual int         receiveMessage(protocol::communication_protocol::Answer& answer, long long int msgID);                    ///< Receives Answer using current WebSocket session. Will fail if there isnt one.
    virtual             protocol::communication_protocol::Answer communicate(protocol::communication_protocol::ClusterMsg &msg, uint8_t retry);     ///< Sends ClusterMsg and receives answer. Same as running CommunicationHandler::sendMessage and CommunicationHandler::receiveMessage
                                                                    ///< but this method also supports reconnect option. If something goes wrong during communication, new connection will be
                                                                    ///< estabilished and the whole process will be repeated.
                                                                    ///< @param retry How many times tries has to be made before returning error.
                                                                    ///< @return Answer protobuf message. If error occures, empty Answer object will be returned.

    static int getInstancesCount();                                 ///< Returns number of CommunicationHander instances.

};

} // namespace veil

#endif // COMMUNICATION_HANDLER_H
