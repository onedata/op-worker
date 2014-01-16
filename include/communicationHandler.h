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

// PB decoder name
#define FUSE_MESSAGES "fuse_messages"
#define COMMUNICATION_PROTOCOL "communication_protocol"

/// How many re-attampts has to be made by CommunicationHandler::communicate
/// before returning error.
#define RECONNECT_COUNT 1

/// Timout for WebSocket handshake
#define CONNECT_TIMEOUT 5000

/// Message receive default timeout
#define RECV_TIMEOUT 2000

/// Path on which cluster listenes for websocket connections
#define CLUSTER_URI_PATH "/veilclient"


typedef websocketpp::client<websocketpp::config::asio_tls_client>       ws_client;
typedef websocketpp::config::asio_tls_client::message_type::ptr         message_ptr;
typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket>          socket_type;

typedef websocketpp::lib::shared_ptr<boost::asio::ssl::context>         context_ptr;
typedef boost::function<void(const veil::protocol::communication_protocol::Answer)>    push_callback;

typedef boost::function<std::string()> get_cert_path_fun;

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
    get_cert_path_fun           m_certFun;
    
    // Container that gathers all incoming messages
    boost::unordered_map<long long, std::string> m_incomingMessages;
    
    // Boost based internals
    boost::shared_ptr<ws_client>                  m_endpoint;
    ws_client::connection_ptr   m_endpointConnection;
    boost::thread               m_worker1;
    boost::thread               m_worker2;
    volatile int                m_connectStatus;    ///< Current connection status
    volatile unsigned int       m_currentMsgId;     ///< Next messageID to be used
    volatile unsigned int       m_errorCount;       ///< How many connection errors were cought
    static volatile int         instancesCount;
    volatile bool               m_isPushChannel;
    std::string                 m_fuseID;           ///< Current fuseID for PUSH channel (if any)
    
    boost::mutex                m_connectMutex;
    boost::condition_variable   m_connectCond;
    boost::mutex                m_msgIdMutex;
    boost::mutex                m_receiveMutex;
    boost::condition_variable   m_receiveCond;
    static boost::mutex         m_instanceMutex;
    
    /// Callback function which shall be called for every cluster PUSH message
    push_callback m_pushCallback;
    
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


    virtual void registerPushChannel(push_callback cb);                     ///< Registers current connection as PUSH channel.
                                                                            ///< @param fuseID used to identify client
                                                                            ///< @param cb Callback function that will be called for every PUSH message
    
    virtual void closePushChannel();                                        ///< Closes PUSH channel. This call notify cluster that this connection shall not be used
                                                                            ///< as PUSH channel

public:
    CommunicationHandler(std::string hostname, int port, std::string certPath);
    virtual ~CommunicationHandler();

    virtual void setCertFun(get_cert_path_fun certFun);                     ///< Setter for function that returns certiificate file path.
    virtual void setFuseID(std::string);                                    ///< Setter for field m_fuseID.
    virtual void setPushCallback(push_callback);                            ///< Setter for field m_pushCallback.
    virtual void enablePushChannel();                                       ///< Enables PUSH channel on this connection. 
                                                                            ///< Note that PUSH callback has to be set with setPushCallback before invocing this method.
    virtual void disablePushChannel();                                      ///< Disables PUSH channel on this connetion.

    virtual bool sendHandshakeACK();                                        ///< Send to cluster given fuse ID. This fuseID will be used with any subsequent message.
    
    virtual unsigned int   getErrorCount();                                 ///< Returns how many communication errors were found
    
    virtual int32_t     getMsgId();                                         ///< Get next message id. Thread safe. All subsequents calls returns next integer value.
    virtual int         openConnection();                                   ///< Opens WebSoscket connection. Returns 0 on success, non-zero otherwise.
    virtual void        closeConnection();                                  ///< Closes active connection.
    virtual int         sendMessage(protocol::communication_protocol::ClusterMsg& message, int32_t msgID = 0);             ///< Sends ClusterMsg using current WebSocket session. Will fail if there isn't one.
                                                                                                                             ///< @return Positive - message ID that shall be used to receive response, negative - error ID
    virtual int         receiveMessage(protocol::communication_protocol::Answer& answer, int32_t msgID, uint32_t timeout = RECV_TIMEOUT);                    ///< Receives Answer using current WebSocket session. Will fail if there isn't one.
    virtual             protocol::communication_protocol::Answer communicate(protocol::communication_protocol::ClusterMsg &msg, uint8_t retry, uint32_t timeout = 0);     ///< Sends ClusterMsg and receives answer. Same as running CommunicationHandler::sendMessage and CommunicationHandler::receiveMessage
                                                                    ///< but this method also supports reconnect option. If something goes wrong during communication, new connection will be
                                                                    ///< estabilished and the whole process will be repeated.
                                                                    ///< @param retry How many times tries has to be made before returning error.
                                                                    ///< @param timeout Timeout for recv operation. By default timeout will be calculated base on frame size
                                                                    ///< @return Answer protobuf message. If error occures, empty Answer object will be returned.

    static int getInstancesCount();                                 ///< Returns number of CommunicationHander instances.

};

} // namespace veil

#endif // COMMUNICATION_HANDLER_H
