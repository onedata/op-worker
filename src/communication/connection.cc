/**
 * @file connection.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "communication/connection.h"

#include "communication/exception.h"
#include "logging.h"

#include <boost/asio.hpp>
#include <openssl/ssl.h>

#include <array>

using namespace std::placeholders;

namespace one {
namespace communication {

Connection::Connection(boost::asio::io_service &ioService,
    boost::asio::ssl::context &context, const bool verifyServerCertificate,
    std::function<std::string()> getHandshake,
    std::function<void(std::string)> onMessageReceived,
    std::function<void(std::shared_ptr<Connection>)> onReady,
    std::function<void(std::shared_ptr<Connection>)> onClosed)
    : m_verifyServerCertificate{verifyServerCertificate}
    , m_getHandshake{std::move(getHandshake)}
    , m_onMessageReceived{std::move(onMessageReceived)}
    , m_onReady{std::move(onReady)}
    , m_onClosed{std::move(onClosed)}
    , m_strand{ioService}
    , m_socket{ioService, context}
{
}

Connection::~Connection()
{
    m_strand.dispatch([this] {
        if (m_socket.lowest_layer().is_open())
            close();
    });
}

void Connection::send(std::string message, std::promise<void> promise)
{
    m_outHeader = htonl(message.size());
    m_outBuffer = std::move(message);
    m_outPromise = std::move(promise);
    m_strand.post(std::bind(&Connection::startWriting, shared_from_this()));
}

void Connection::start(boost::asio::ip::tcp::resolver::iterator endpointIt)
{
    m_strand.dispatch([this, endpointIt] {
        boost::asio::async_connect(
            m_socket.lowest_layer(), endpointIt,
            [t = shared_from_this()](const boost::system::error_code &ec,
                boost::asio::ip::tcp::resolver::iterator) {

                if (ec) {
                    t->close("Failed to establish TCP connection", ec);
                    return;
                }

                t->m_socket.lowest_layer().set_option(
                    boost::asio::ip::tcp::no_delay{true});

                t->m_socket.async_handshake(
                    boost::asio::ssl::stream_base::client,
                    [t](const boost::system::error_code &ec) {
                        if (ec) {
                            auto verifyResult = SSL_get_verify_result(
                                t->m_socket.native_handle());

                            if (verifyResult != 0 &&
                                t->m_verifyServerCertificate) {
                                t->close("Server certificate verification "
                                         "failed. OpenSSL error",
                                    ec);
                            }
                            else {
                                t->close("Failed to perform SSL handshake", ec);
                            }

                            return;
                        }

                        // Start by sending a handshake with an empty promise,
                        // after which onReady will be called.
                        t->send(m_getHandshake(), std::promise<void>{});
                        t->startReading();
                    });
            });
    });
}

void Connection::close()
{
    m_strand.dispatch([this] {
        boost::system::error_code ec;
        auto logOnError = [&ec](auto what) {
            if (ec)
                LOG(WARNING) << what << ": " << ec.message();
        };

        m_socket.shutdown(ec);
        logOnError("Failed to shutdown SSL connection layer");

        m_socket.lowest_layer().shutdown(
            boost::asio::ip::tcp::socket::shutdown_both, ec);
        logOnError("Failed to shutdown TCP connection");

        m_socket.lowest_layer().close(ec);
        logOnError("Failed to cleanly close TCP socket");

        m_onClosed(shared_from_this());
    });
}

void Connection::startReading()
{
    boost::asio::async_read(m_socket, headerToBuffer(m_inHeader),
        m_strand.wrap([t = shared_from_this()](
            const boost::system::error_code &ec, size_t) {

            if (ec) {
                t->close("Failed to read message header", ec);
                return;
            }

            const auto messageSize = ntohl(t->m_inHeader);
            t->m_inBuffer.resize(messageSize);

            boost::asio::async_read(t->m_socket,
                boost::asio::mutable_buffers_1{
                    &t->m_inBuffer[0], t->m_inBuffer.size()},
                t->m_strand.wrap(
                    [t](const boost::system::error_code &ec, size_t) {
                        if (ec) {
                            t->close("Failed to read message body", ec);
                            return;
                        }

                        t->m_onMessageReceived(std::move(t->m_inBuffer));
                        t->startReading();
                    }));
        }));
}

void Connection::startWriting()
{
    std::array<boost::asio::const_buffer, 2> compositeBuffer{
        {headerToBuffer(m_outHeader), boost::asio::buffer(m_outBuffer)}};

    boost::asio::async_write(m_socket, compositeBuffer,
        m_strand.wrap([t = shared_from_this()](
            const boost::system::error_code &ec, size_t) {

            if (ec) {
                auto msg = t->close("Failed to write message", ec);
                auto e = std::make_exception_ptr(SendError{msg});
                t->m_outPromise.set_exception(e);
                return;
            }

            t->m_outPromise.set_value();
            t->m_onReady(t);
        }));
}

std::string Connection::close(
    std::string what, const boost::system::error_code &ec)
{
    auto msg = what + ": " + ec.message();
    LOG(ERROR) << msg;
    close();
    return msg;
}

boost::asio::mutable_buffers_1 Connection::headerToBuffer(std::uint32_t &header)
{
    return {static_cast<void *>(&header), sizeof(header)};
}

} // namespace communication
} // namespace one
