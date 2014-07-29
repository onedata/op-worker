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

/**
 * A @c veil::communication::ConnectionPool specialization for managing
 * TLS WebSocket++ based connections.
 */
class WebsocketConnectionPool: public ConnectionPool
{
    using endpoint_type = websocketpp::client<websocketpp::config::asio_tls_client>;
    using socket_type = websocketpp::transport::asio::tls_socket::connection::socket_type;
    using context_type = boost::asio::ssl::context;
    using context_ptr = std::shared_ptr<context_type>;

public:
    /**
     * Constructor.
     * Starts an underlying ASIO endpoint with a worker thread.
     * @param connectionsNumber Number of connections that should be maintained
     * by this pool.
     * @param uri Server's URI to connect to.
     * @param certificateData Certificate data to use for SSL authentication.
     * @param verifyServerCertificate Determines whether to verify server's
     * certificate.
     */
    WebsocketConnectionPool(
            const unsigned int connectionsNumber,
            std::string uri,
            std::shared_ptr<const CertificateData> certificateData,
            const bool verifyServerCertificate);

    /**
     * Destructor.
     * Stops the underlying ASIO endpoint and the worker thread and closes
     * maintained connections.
     */
    ~WebsocketConnectionPool();

protected:
    /**
     * Creates a new instance of @c veil::communication::WebsocketConnection .
     * @return A new instance of @c WebsocketConnection .
     */
    std::unique_ptr<Connection> createConnection() override;

private:
    /**
     * @defgroup WebSocket++ handlers.
     * @see websocketpp::transport::asio::tls_socket::endpoint
     * @{
     */
    context_ptr onTLSInit();
    void onSocketInit(socket_type &socket);
    /**@}*/

    std::thread m_ioThread;
    endpoint_type m_endpoint;
    const std::shared_ptr<const CertificateData> m_certificateData;
    const bool m_verifyServerCertificate;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATION_WEBSOCKET_CONNECTION_POOL_H
