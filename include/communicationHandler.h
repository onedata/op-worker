/**
 * @file communicationHandler.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */


#ifndef COMMUNICATION_HANDLER_H
#define COMMUNICATION_HANDLER_H

#include <numeric>
#include <string>
#include <boost/atomic.hpp>
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

static const unsigned int MAX_GENERATED_MSG_ID = std::numeric_limits<unsigned int>::max() / 2;
static const unsigned int IGNORE_ANSWER_MSG_ID = MAX_GENERATED_MSG_ID + 1;

typedef websocketpp::client<websocketpp::config::asio_tls_client>       ws_client;
typedef websocketpp::config::asio_tls_client::message_type::ptr         message_ptr;
typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket>          socket_type;

typedef websocketpp::lib::shared_ptr<boost::asio::ssl::context>         context_ptr;
typedef boost::function<void(const veil::protocol::communication_protocol::Answer)>    push_callback;
typedef boost::unique_lock<boost::recursive_mutex> unique_lock;


template<typename T>
std::string toString(T in) {
    std::ostringstream ss;
    ss << in;
    return ss.str();
}

namespace veil {


/// CertificateInfo provides information about certificate configuration
/// including: certificate type, certificate paths and / or certificate
/// internal buffers pointing to certs loaded into program memory.
struct CertificateInfo {
    enum CertificateType {
        PEM,
        P12,
        ASN1
    };

    std::string         user_cert_path;     ///< Path to cert chain file
    std::string         user_key_path;      ///< Path to user key file
    CertificateType     cert_type;

    boost::asio::const_buffer chain_data;   ///< Buffer containing cert chain (PEM format required)
    boost::asio::const_buffer key_data;     ///< Buffer containing user key (PEM format required)

    /// Construct CertificateInfo using file paths
    CertificateInfo(std::string         p_user_cert_path,
                    std::string         p_user_key_path,
                    CertificateType     p_cert_type = PEM)
      : user_cert_path(p_user_cert_path),
        user_key_path(p_user_key_path),
        cert_type(p_cert_type)
    {
    }

    /// Construct CertificateInfo using memeory buffers
    CertificateInfo(boost::asio::const_buffer chain_buff, boost::asio::const_buffer key_buff)
      : cert_type(PEM),
        chain_data(chain_buff),
        key_data(key_buff)
    {
    }
};

// getter for CertificateInfo struct
typedef boost::function<CertificateInfo()> cert_info_fun;

/**
 * The CommunicationHandler class.
 * Object of this class represents WebSocket based connection to cluster.
 * The CommunicationHandler allows to communicate with cluster by sending and receiving
 * ClusterMsg messages.
 */
class CommunicationHandler
{
private:
    boost::atomic<error::Error> m_lastError;
    const bool m_checkCertificate;

protected:

    std::string                 m_hostname;
    int                         m_port;
    cert_info_fun               m_getCertInfo;

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
    volatile bool               m_isPushChannel;
    std::string                 m_fuseID;           ///< Current fuseID for PUSH channel (if any)
    static SSL_SESSION*         m_session;

    boost::recursive_mutex      m_connectMutex;
    boost::recursive_mutex      m_reconnectMutex;
    boost::condition_variable_any   m_connectCond;
    boost::recursive_mutex      m_msgIdMutex;
    boost::recursive_mutex      m_receiveMutex;
    boost::condition_variable_any   m_receiveCond;
    static boost::recursive_mutex m_instanceMutex;

    uint64_t                    m_lastConnectTime;

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

    /// Current connection status codes
    enum ConnectionStatus
    {
        UNDERLYING_LIB_ERROR= -5,
        TIMEOUT             = -4,
        HANDSHAKE_ERROR     = -3,
        TRANSPORT_ERROR     = -2,
        CLOSED              = -1,
        CONNECTED           = 0,
        NO_ERROR            = 0
    };

    CommunicationHandler(const std::string &hostname, int port, cert_info_fun, const bool checkCertificate);
    virtual ~CommunicationHandler();

    virtual void setCertFun(cert_info_fun certFun);                         ///< Setter for function that returns CommunicationHandler::CertificateInfo struct.
    virtual void setFuseID(const std::string&);                             ///< Setter for field m_fuseID.
    virtual void setPushCallback(push_callback);                            ///< Setter for field m_pushCallback.
    virtual void enablePushChannel();                                       ///< Enables PUSH channel on this connection.
                                                                            ///< Note that PUSH callback has to be set with setPushCallback before invocing this method.
    virtual void disablePushChannel();                                      ///< Disables PUSH channel on this connetion.

    virtual bool sendHandshakeACK();                                        ///< Send to cluster given fuse ID. This fuseID will be used with any subsequent message.

    virtual unsigned int   getErrorCount();                                 ///< Returns how many communication errors were found

    virtual int32_t     getMsgId();                                         ///< Get next message id. Thread safe. All subsequents calls returns next integer value.
    virtual int         openConnection();                                   ///< Opens WebSoscket connection. Returns 0 on success, non-zero otherwise.
    virtual void        closeConnection();                                  ///< Closes active connection.

    /// Sends ClusterMsg using current WebSocket session. Will fail if there isn't one. No throw version.
    /// @param ec error code (CommunicationHandler::ConnectionStatus)
    /// @return message ID that shall be used to receive response
    virtual int32_t     sendMessage(protocol::communication_protocol::ClusterMsg& message, int32_t msgID, ConnectionStatus &ec);

    /// Sends ClusterMsg using current WebSocket session. Will fail if there isn't one. Throws CommunicationHandler::ConnectionStatus on error.
    /// @return message ID that shall be used to receive response
    virtual int32_t     sendMessage(protocol::communication_protocol::ClusterMsg& message, int32_t msgID = 0);

    /// Receives Answer using current WebSocket session. Will fail if there isn't one.
    virtual int         receiveMessage(protocol::communication_protocol::Answer& answer, int32_t msgID, uint32_t timeout = RECV_TIMEOUT);

    /**
     * Sends ClusterMsg and receives answer. Same as running CommunicationHandler::sendMessage and CommunicationHandler::receiveMessage
     * but this method also supports reconnect option. If something goes wrong during communication, new connection will be
     * estabilished and the whole process will be repeated.
     * @param retry How many times tries has to be made before returning error.
     * @param timeout Timeout for recv operation. By default timeout will be calculated base on frame size
     * @return Answer protobuf message. If error occures, empty Answer object will be returned.
     */
    virtual             protocol::communication_protocol::Answer communicate(protocol::communication_protocol::ClusterMsg &msg, uint8_t retry, uint32_t timeout = 0);

    /**
     * @returns Last error encountered by the connection.
     */
    error::Error getLastError() const;
};

} // namespace veil

#endif // COMMUNICATION_HANDLER_H
