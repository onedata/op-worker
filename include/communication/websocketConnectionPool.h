/**
 * @file websocketConnectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATION_WEBSOCKET_CONNECTION_POOL_H
#define VEILHELPERS_COMMUNICATION_WEBSOCKET_CONNECTION_POOL_H


#include "connectionPool.h"

#include "certificateData.h"

#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_client.hpp>

#include <thread>
#include <unordered_set>

extern template class websocketpp::client<websocketpp::config::asio_tls_client>;

namespace veil
{
namespace communication
{

class WebsocketConnection;

class WebsocketConnectionPool: public ConnectionPool
{
    using endpoint_type = websocketpp::client<websocketpp::config::asio_tls_client>;
    using socket_type = websocketpp::transport::asio::tls_socket::connection::socket_type;
    using context_type = boost::asio::ssl::context;
    using context_ptr = std::shared_ptr<context_type>;

public:
    WebsocketConnectionPool(const unsigned int connectionsNumber,
                            std::string uri,
                            std::shared_ptr<const CertificateData> certificateData,
                            const bool verifyServerCertificate);

    ~WebsocketConnectionPool();
    WebsocketConnectionPool(WebsocketConnectionPool&&) = default;
    WebsocketConnectionPool &operator=(WebsocketConnectionPool&&) & = default;
    WebsocketConnectionPool(const WebsocketConnectionPool&) = delete;
    WebsocketConnectionPool &operator=(const WebsocketConnectionPool&) = delete;

protected:
    std::unique_ptr<Connection> createConnection() override;

private:
    context_ptr onTLSInit();                 ///< On TLS init callback
    void onSocketInit(socket_type &socket);  ///< On socket init callback

    std::thread m_ioThread;
    endpoint_type m_endpoint;
    const std::shared_ptr<const CertificateData> m_certificateData;
    const bool m_verifyServerCertificate;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATION_WEBSOCKET_CONNECTION_POOL_H
