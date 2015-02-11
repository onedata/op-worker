/**
 * @file sslConnectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "communication/sslConnectionPool.h"

#include "communication/exception.h"
#include "logging.h"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

#include <algorithm>
#include <array>
#include <iterator>
#include <tuple>

using steady_timer =
    boost::asio::basic_waitable_timer<std::chrono::steady_clock>;

using namespace std::literals::chrono_literals;

static constexpr auto RECREATE_DELAY = 5s;

namespace {
template <typename lambda>
std::shared_ptr<lambda> make_shared_handler(lambda &&l)
{
    return std::make_shared<lambda>(std::forward<lambda>(l));
}
}

namespace one {
namespace communication {

SSLConnectionPool::SSLConnectionPool(
    const unsigned int connectionsNumber, std::string host, std::string port,
    std::shared_ptr<const CertificateData> certificateData,
    const bool verifyServerCertificate)
    : m_certificateData{std::move(certificateData)}
    , m_verifyServerCertificate{verifyServerCertificate}
    , m_idleWork{m_ioService}
    , m_blockingStrand{m_ioService}
    , m_context{boost::asio::ssl::context::tlsv12_client}
{
    m_outbox.set_capacity(OUTBOX_SIZE);

    std::generate_n(std::back_inserter(m_workers), connectionsNumber, [this]{
        return std::thread{[this] { m_ioService.run(); }};
    });

    boost::asio::ip::tcp::resolver resolver{m_ioService};
    boost::asio::ip::tcp::resolver::query query{host, port};

    boost::system::error_code ec;
    m_endpointIterator = resolver.resolve(query, ec);
    if (ec)
        throw ConnectionError{ec.message()};

    // TODO: Set up an ssl context

    for(auto i = 0u; i < connectionsNumber; ++i)
        createConnection();
}

std::future<void> SSLConnectionPool::send(std::string message)
{
    std::promise<void> promise;
    auto future = promise.get_future();
    m_outbox.emplace(std::make_shared<std::tuple<std::string, std::promise<void>>>(
        std::forward_as_tuple(std::move(message), std::move(promise))));
    return future;
}

void SSLConnectionPool::write(SSLConnectionPool::Connection conn,
                              std::string message, std::promise<void> promise)
{
    // Data needs to be persisted until write handler is called
    auto header = std::make_unique<std::uint32_t>(htonl(message.size()));
    auto msg = std::make_unique<std::string>(std::move(message));

    std::array<boost::asio::const_buffer, 2> buffers{
        {boost::asio::buffer(static_cast<void *>(&*header), sizeof(*header)),
         boost::asio::buffer(*msg)}};

    auto handler = make_shared_handler([
        this,
        conn = std::move(conn),
        header = std::move(header),
        msg = std::move(msg),
        promise = std::move(promise)
    ](const boost::system::error_code & ec) mutable
    {
        if (ec) {
            onOperationError(std::move(conn), std::move(promise), ec);
            return;
        }

        promise.set_value();
        returnToQueue(std::move(conn));
    });

    boost::asio::async_write(*conn, buffers,
                             [handler](const boost::system::error_code &ec,
                                       size_t) { (*handler)(ec); });
}

void SSLConnectionPool::returnToQueue(SSLConnectionPool::Connection conn)
{
    auto blockHandler = make_shared_handler([ this, conn = std::move(conn) ]() mutable
    {
        std::shared_ptr<std::tuple<std::string, std::promise<void>>> data;
        m_outbox.pop(data);

        auto handler = make_shared_handler([
            this,
            conn = std::move(conn),
            message = std::move(std::get<0>(*data)),
            promise = std::move(std::get<1>(*data))
        ]() mutable
        {
            write(std::move(conn), std::move(message), std::move(promise));
        });

        m_ioService.post([handler]{ (*handler)(); });
    });

    m_blockingStrand.post([blockHandler] { (*blockHandler)(); });
}

void SSLConnectionPool::createConnection()
{
    Connection conn{std::make_unique<SSLSocket>(m_ioService, m_context)};
    conn->set_verify_mode(m_verifyServerCertificate
                              ? boost::asio::ssl::verify_peer
                              : boost::asio::ssl::verify_none);
    // TODO: conn->set_verify_callback()

    auto connectHandler = make_shared_handler(
        [ this, conn = std::move(conn) ](const boost::system::error_code & ec) mutable {
                                            if (ec) {
                                                LOG(ERROR)
                                                    << "Failed to connect: "
                                                    << ec.message();
                                                scheduleRecreateConnection();
                                                return;
                                            }

                                            auto handshakeHandler = make_shared_handler(
             [ this,
               conn = std::move(conn) ](const boost::system::error_code & ec) mutable {
                                           if (ec) {
                                               LOG(ERROR) << "SSL Handshake "
                                                             "failed: "
                                                          << ec.message();
                                               scheduleRecreateConnection();
                                               return;
                                           }

                                           returnToQueue(std::move(conn));
                                       });

                                            conn->async_handshake(
                                                boost::asio::ssl::stream_base::
                                                    client,
                                                [handshakeHandler](
                                                    const boost::system::
                                                        error_code &ec) {
                                                    (*handshakeHandler)(ec);
                                                });
                                        });

    boost::asio::async_connect(
        conn->lowest_layer(), m_endpointIterator,
        [connectHandler](const boost::system::error_code &ec, boost::asio::ip::tcp::resolver::iterator) mutable {
            (*connectHandler)(ec);
        });
}

void SSLConnectionPool::onOperationError(Connection conn,
                                         std::promise<void> brokenPromise,
                                         const boost::system::error_code &ec)
{
    auto e = std::make_exception_ptr(ConnectionError{ec.message()});
    brokenPromise.set_exception(e);
    scheduleRecreateConnection();
}

void SSLConnectionPool::scheduleRecreateConnection()
{
    auto timer = std::make_unique<steady_timer>(m_ioService, RECREATE_DELAY);
    auto handler = make_shared_handler([
        this,
        t = std::move(timer)
    ] { createConnection(); });

    timer->async_wait([handler](const boost::system::error_code &ec) {
        (*handler)();
    });
}

SSLConnectionPool::~SSLConnectionPool()
{
    m_ioService.stop();

}

SSLConnectionPool::Connection::Connection(std::unique_ptr<SSLConnectionPool::SSLSocket> socket)
    : m_socket{std::move(socket)}
{
}

void SSLConnectionPool::Connection::close()
{
    boost::system::error_code ec;

    m_socket->shutdown(ec);
    if (ec)
        LOG(WARNING) << "Failed to shutdown SSL on a connection: "
                     << ec.message();

    m_socket->lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                                   ec);
    if (ec)
        LOG(WARNING) << "Failed to shutdown TCP connection: " << ec.message();

    m_socket->lowest_layer().close(ec);
    if (ec)
        LOG(WARNING) << "Failed to cleanly close TCP socket: " << ec.message();
}

SSLConnectionPool::Connection::~Connection()
{
    if (m_socket)
        close();
}

SSLConnectionPool::SSLSocket &SSLConnectionPool::Connection::operator*()
{
    return *m_socket;
}

std::unique_ptr<SSLConnectionPool::SSLSocket> &SSLConnectionPool::Connection::operator->()
{
    return m_socket;
}

} // namespace communication
} // namespace one
