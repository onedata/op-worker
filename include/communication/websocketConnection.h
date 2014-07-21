/**
 * @file websocketConnection.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATION_WEBSOCKET_CONNECTION_H
#define VEILHELPERS_COMMUNICATION_WEBSOCKET_CONNECTION_H


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

class WebsocketConnection: public Connection
{
    using config_type = websocketpp::config::asio_tls_client;
    using endpoint_type = websocketpp::client<config_type>;
    using connection_ptr = endpoint_type::connection_ptr;
    using message_ptr = config_type::message_type::ptr;

public:
    WebsocketConnection(std::function<void(const std::string&)> onMessageCallback,
                        std::function<void(Connection&)> onFailCallback,
                        std::function<void(Connection&)> onOpenCallback,
                        std::function<void(Connection&)> onErrorCallback,
                        endpoint_type &endpoint,
                        const std::string &uri,
                        std::shared_ptr<const CertificateData> certificateData,
                        const bool verifyServerCertificate);

    ~WebsocketConnection();
    WebsocketConnection(const WebsocketConnection&) = delete;
    WebsocketConnection &operator=(const WebsocketConnection&) = delete;

    void send(const std::string &payload) override;

private:
    void onMessage(message_ptr msg);         ///< Incoming WebSocket message callback
    void onOpen();                           ///< WebSocket connection opened
    void onClose();                          ///< WebSocket connection closed
    void onFail();                           ///< WebSocket connection failed callback. This can proc only before CommunicationHandler::onOpen
    bool onPing(std::string);                ///< Ping received callback
    void onPong(std::string);                ///< Pong received callback
    void onPongTimeout(std::string);         ///< Cluaster failed to respond on ping message
    void onInterrupt();                      ///< WebSocket connection was interuped

    endpoint_type &m_endpoint;
    const std::shared_ptr<const CertificateData> m_certificateData;
    const bool m_verifyServerCertificate;
    websocketpp::connection_hdl m_connection;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATION_WEBSOCKET_CONNECTION_H
