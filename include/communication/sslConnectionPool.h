/**
 * @file sslConnectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_SSL_CONNECTION_POOL_H
#define HELPERS_COMMUNICATION_SSL_CONNECTION_POOL_H

#include "communication/connectionPool.h"

#include <boost/asio/io_service.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <tbb/concurrent_queue.h>

#include <functional>
#include <future>
#include <queue>
#include <memory>
#include <unordered_set>
#include <string>
#include <thread>
#include <tuple>

namespace one {
namespace communication {

class CertificateData;

/**
 * A @c one::communication::ConnectionPool specialization for managing
 * SSL based connections.
 */
class SSLConnectionPool {
    // TODO: Chyba nie potrzeba używać unique_ptr, socket jest movable
    using SSLSocket = boost::asio::ssl::stream<boost::asio::ip::tcp::socket>;
    static constexpr size_t OUTBOX_SIZE = 1000;

    class Connection {
    public:
        Connection(std::unique_ptr<SSLSocket> socket);
        Connection(Connection&&) = default;
        Connection &operator=(Connection&&) = default;
        void close();
        SSLSocket &operator*();
        std::unique_ptr<SSLSocket> &operator->();
        ~Connection();

    private:
        std::unique_ptr<SSLSocket> m_socket;
    };

public:
    /**
     * Constructor.
     * Starts an underlying ASIO endpoint with a worker thread.
     * @param connectionsNumber Number of connections that should be maintained
     * by this pool.
     * @param host Server's hostname to connect to.
     * @param port Server's port to connect to.
     * @param certificateData Certificate data to use for SSL authentication.
     * @param verifyServerCertificate Determines whether to verify server's
     * certificate.
     */
    SSLConnectionPool(const unsigned int connectionsNumber, std::string host,
                      std::string port,
                      std::shared_ptr<const CertificateData> certificateData,
                      const bool verifyServerCertificate);

    std::future<void> send(std::string message);

    void createConnection();

    /**
     * Destructor.
     * Stops the underlying ASIO endpoint and the worker thread and closes
     * maintained connections.
     */
    ~SSLConnectionPool();

private:
    void onOperationError(Connection conn, std::promise<void> brokenPromise,
                          const boost::system::error_code &ec);

    void scheduleRecreateConnection();

    void write(Connection conn, std::string message,
               std::promise<void> promise);

    void returnToQueue(Connection conn);

    std::shared_ptr<const CertificateData> m_certificateData;
    const bool m_verifyServerCertificate;

    boost::asio::io_service m_ioService;
    boost::asio::io_service::work m_idleWork;
    boost::asio::io_service::strand m_blockingStrand;
    std::vector<std::thread> m_workers;

    boost::asio::ip::tcp::resolver::iterator m_endpointIterator;
    tbb::concurrent_bounded_queue<
        std::shared_ptr<std::tuple<std::string, std::promise<void>>>> m_outbox;
    boost::asio::ssl::context m_context;
};

} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_SSL_CONNECTION_POOL_H
