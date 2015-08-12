/**
 * @file connectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_CONNECTION_POOL_H
#define HELPERS_COMMUNICATION_CONNECTION_POOL_H

#include "connection.h"

#include <asio/io_service.hpp>
#include <asio/strand.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/ssl/context.hpp>
#include <asio/ssl/stream.hpp>
#include <tbb/concurrent_queue.h>

#include <functional>
#include <memory>
#include <string>
#include <queue>
#include <vector>
#include <system_error>
#include <thread>
#include <tuple>
#include <unordered_set>

namespace one {

class IoServiceExecutor;

namespace communication {

namespace cert {
class CertificateData;
}

class Connection;

/**
 * A @c ConnectionPool is responsible for managing instances of @c Connection.
 * It provides a facade for the connections, ensuring that outside entities
 * do not interact with connections directly.
 */
class ConnectionPool {
    using SendTask =
        std::tuple<std::string, std::function<void(const std::error_code &)>>;

    using ConnectionFactory = std::function<Connection::Ptr(asio::io_service &,
        asio::ssl::context &, const bool, std::function<std::string()> &,
        std::function<std::error_code(std::string)> &,
        std::function<void(std::string)>,
        std::function<void(Connection::Ptr, const std::error_code &)>)>;

public:
    using Callback = std::function<void(const std::error_code &)>;
    enum class ErrorPolicy { ignore, propagate };

    /**
     * A reference to @c *this typed as a @c ConnectionPool.
     */
    ConnectionPool &connectionPool = *this;

    /**
     * Constructor.
     * @param connectionsNumber Number of connections that should be maintained
     * by this pool.
     * @param host Hostname of the remote endpoint.
     * @param service Name of well-known service provided by the remote
     * endpoint, or a port number.
     * @param verifyServerCertificate Specifies whether to verify server's
     * SSL certificate.
     * @param certificateData Certificate data to use for SSL authentication.
     */
    ConnectionPool(const unsigned int connectionsNumber, std::string host,
        std::string service, const bool verifyServerCertificate,
        ConnectionFactory connectionFactory,
        ErrorPolicy errorPolicy = ErrorPolicy::ignore);

    /**
     * Creates connections and threads that will work for them.
     * May throw a connection-related exception.
     * @note This method is separated from the constructor so that the
     * initialization can be augmented by other communication layers.
     */
    void connect();

    /**
     * Sets handshake-related functions.
     * The handshake functions are passed down to connections and used on
     * initialization of each TCP connection.
     * @param getHandshake A function that returns a handshake to send through
     * connections.
     * @param onHandshakeResponse A function that takes a handshake response.
     * @note This method is separated from constructor so that the handshake
     * messages can be translated by other communication layers.
     */
    void setHandshake(std::function<std::string()> getHandshake,
        std::function<std::error_code(std::string)> onHandshakeResponse);

    /**
     * Sets a function to handle received messages.
     * @param onMessage The received message.
     */
    void setOnMessageCallback(std::function<void(std::string)> onMessage);

    /**
     * Sets certificate data to be used to authorize the client.
     * @param certificateData The certificate data to set.
     */
    void setCertificateData(
        std::shared_ptr<cert::CertificateData> certificateData);

    /**
     * Sends a message through one of the managed connections.
     * @param message The message to send.
     * @return A future fulfilled when the message is sent or (with an
     * exception) when an error occured.
     */
    void send(std::string message, Callback callback, const int = int{});

    /**
     * Destructor.
     * Stops the underlying asio:: endpoint and the worker thread and
     * closes maintained connections.
     */
    virtual ~ConnectionPool();

private:
    void createConnection();
    void onMessageReceived(std::string message);
    void onConnectionReady(Connection::Ptr conn);
    void onConnectionClosed(Connection::Ptr conn, const std::error_code &ec);

    const unsigned int m_connectionsNumber;
    std::string m_host;
    std::string m_service;
    const bool m_verifyServerCertificate;
    ConnectionFactory m_connectionFactory;
    ErrorPolicy m_errorPolicy;
    std::shared_ptr<const cert::CertificateData> m_certificateData;

    std::function<std::string()> m_getHandshake;
    std::function<std::error_code(std::string)> m_onHandshakeResponse;
    std::function<void(std::string)> m_onMessage = [](auto) {};

    asio::io_service m_ioService;
    asio::io_service::work m_idleWork;
    asio::io_service::strand m_blockingStrand;
    asio::io_service::strand m_connectionsStrand;

private:
    std::vector<std::thread> m_workers;
    asio::ssl::context m_context;

    tbb::concurrent_bounded_queue<std::shared_ptr<SendTask>> m_outbox;
    tbb::concurrent_queue<std::shared_ptr<SendTask>> m_rejects;
    std::unordered_set<Connection::Ptr> m_connections;
};

} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_CONNECTION_POOL_H
