/**
 * @file websocketConnection.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_WEBSOCKET_CONNECTION_H
#define VEILHELPERS_WEBSOCKET_CONNECTION_H


#include "connection.h"

#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_client.hpp>
#include <websocketpp/connection.hpp>

#include <memory>
#include <string>

extern template class websocketpp::client<websocketpp::config::asio_tls_client>;

namespace veil
{
namespace communication
{

class CertificateData;
class Mailbox;

class WebsocketConnection: public Connection
{
    using config_type = websocketpp::config::asio_tls_client;
    using endpoint_type = websocketpp::client<config_type>;
    using connection_ptr = endpoint_type::connection_ptr;
    using context_type = boost::asio::ssl::context;
    using context_ptr = std::shared_ptr<context_type>;
    using message_ptr = config_type::message_type::ptr;
    using socket_type = websocketpp::transport::asio::tls_socket::connection::socket_type;

public:
    WebsocketConnection(std::shared_ptr<Mailbox> mailbox,
                        std::function<void(std::shared_ptr<Connection>)> onFailCallback,
                        std::function<void(std::shared_ptr<Connection>)> onOpenCallback,
                        std::function<void(std::shared_ptr<Connection>)> onErrorCallback,
                        std::shared_ptr<endpoint_type> endpoint,
                        const std::string &uri,
                        std::shared_ptr<CertificateData> certificateData,
                        const bool verifyServerCertificate);

    void send(const std::string &payload) override;
    void close() override;

private:
    context_ptr onTLSInit();                 ///< On TLS init callback
    void onSocketInit(socket_type &socket);  ///< On socket init callback
    void onMessage(message_ptr msg);         ///< Incoming WebSocket message callback
    void onOpen();                           ///< WebSocket connection opened
    void onClose();                          ///< WebSocket connection closed
    void onFail();                           ///< WebSocket connection failed callback. This can proc only before CommunicationHandler::onOpen
    bool onPing(std::string);                ///< Ping received callback
    void onPong(std::string);                ///< Pong received callback
    void onPongTimeout(std::string);         ///< Cluaster failed to respond on ping message
    void onInterrupt();                      ///< WebSocket connection was interuped

    websocketpp::connection_hdl m_connection;
    std::weak_ptr<endpoint_type> m_endpoint;
    const std::shared_ptr<CertificateData> m_certificateData;
    const bool m_verifyServerCertificate;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_WEBSOCKET_CONNECTION_H
