/**
 * @file persistentConnection.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "persistentConnection.h"

#include "exception.h"
#include "logging.h"

#include <asio.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>
#include <openssl/ssl.h>

#include <future>

using namespace std::placeholders;
using namespace std::literals;

namespace one {
namespace communication {

PersistentConnection::PersistentConnection(std::string host,
    const unsigned short port, asio::ssl::context &context,
    std::function<void(std::string)> onMessage,
    std::function<void(PersistentConnection &)> onReady,
    std::function<std::string()> getHandshake,
    std::function<std::error_code(std::string)> onHandshakeResponse,
    std::function<void(std::error_code)> onHandshakeDone)
    : m_host{std::move(host)}
    , m_port{port}
    , m_context{context}
    , m_onMessage{std::move(onMessage)}
    , m_onReady{std::move(onReady)}
    , m_getHandshake{std::move(getHandshake)}
    , m_onHandshakeResponse{std::move(onHandshakeResponse)}
    , m_callback{std::move(onHandshakeDone)}
{
}

PersistentConnection::~PersistentConnection()
{
    if (m_socket) {
        auto promise = std::make_shared<std::promise<void>>();
        auto future = promise->get_future();

        m_socket->closeAsync(
            m_socket, {[=]() mutable { promise->set_value(); },
                       [=](auto) mutable { promise->set_value(); }});

        // After wait_for returns, proceed with the shutdown whether the TCP
        // connection has been gracefully shut down or not.
        future.wait_for(SHUTDOWN_TIMEOUT);
    }
}

void PersistentConnection::connect()
{
    m_socket = std::make_shared<etls::TLSSocket>(m_app, m_context);
    m_socket->connectAsync(m_socket, m_host, m_port,
        createCallback<etls::TLSSocket::Ptr>([=](auto) { this->onConnect(); }));
}

void PersistentConnection::onConnect()
{
    if (!m_getHandshake) {
        start();
        return;
    }

    auto buffer = prepareOutBuffer(m_getHandshake());
    m_socket->sendAsync(
        m_socket, buffer, createCallback([=] { onHandshakeSent(); }));
}

void PersistentConnection::onHandshakeSent()
{
    asyncRead([=](auto) { this->onHandshakeReceived(); });
}

void PersistentConnection::onHandshakeReceived()
{
    std::error_code ec;
    if (m_onHandshakeResponse)
        ec = m_onHandshakeResponse(std::move(m_inData));

    if (ec)
        onError(ec);
    else
        start();
}

void PersistentConnection::onSent()
{
    notify();
    m_onReady(*this);
}

void PersistentConnection::onError(const std::error_code &ec)
{
    notify(ec);

    m_connected = false;
    m_socket->shutdownAsync(
        m_socket, asio::ip::tcp::socket::shutdown_both, {[] {}, [](auto) {}});
    m_socket.reset();

    auto timer =
        std::make_shared<asio::steady_timer>(m_app.ioService(), RECREATE_DELAY);

    timer->async_wait([this, timer](auto ec) {
        if (!ec)
            this->connect();
    });
}

void PersistentConnection::send(std::string message, Callback callback)
{
    asio::post(m_app.ioService(), [
        =,
        message = std::move(message),
        callback = std::move(callback)
    ]() mutable {
        if (!m_connected) {
            callback(asio::error::not_connected);
            return;
        }

        m_callback = std::move(callback);
        auto buffer = prepareOutBuffer(std::move(message));
        m_socket->sendAsync(
            m_socket, buffer, createCallback([=] { onSent(); }));
    });
}

std::array<asio::const_buffer, 2> PersistentConnection::prepareOutBuffer(
    std::string message)
{
    m_outHeader = htonl(message.size());
    m_outData = std::move(message);
    return {{headerToBuffer(m_outHeader), asio::buffer(m_outData)}};
}

void PersistentConnection::readLoop()
{
    asyncRead([=](asio::mutable_buffer) {
        m_onMessage(std::move(m_inData));
        readLoop();
    });
}

void PersistentConnection::notify(const std::error_code &ec)
{
    if (m_callback) {
        decltype(m_callback) callback;
        std::swap(callback, m_callback);
        callback(ec);
    }
    if (!m_connected && m_onHandshakeDone) {
        m_onHandshakeDone(ec);
    }
}

void PersistentConnection::start()
{
    notify();
    m_connected = true;
    readLoop();
    m_onReady(*this);
}

template <typename... Args, typename SF>
etls::Callback<Args...> PersistentConnection::createCallback(SF &&onSuccess)
{
    return etls::Callback<Args...>(std::forward<SF>(onSuccess),
        std::bind(&PersistentConnection::onError, this, _1));
}

template <typename SF> void PersistentConnection::asyncRead(SF &&onSuccess)
{
    auto onHeaderSuccess = [ =, onSuccess = std::forward<SF>(onSuccess) ](
        asio::mutable_buffer) mutable
    {
        const std::size_t size = ntohl(m_inHeader);
        m_inData.resize(size);

        m_socket->recvAsync(m_socket, asio::buffer(m_inData),
            createCallback<asio::mutable_buffer>(std::move(onSuccess)));
    };

    m_socket->recvAsync(m_socket, headerToBuffer(m_inHeader),
        createCallback<asio::mutable_buffer>(std::move(onHeaderSuccess)));
}

asio::mutable_buffers_1 PersistentConnection::headerToBuffer(
    std::uint32_t &header)
{
    return {static_cast<void *>(&header), sizeof(header)};
}

std::unique_ptr<PersistentConnection> createConnection(std::string host,
    const unsigned short port, asio::ssl::context &context,
    std::function<void(std::string)> onMessage,
    std::function<void(PersistentConnection &)> onReady,
    std::function<std::string()> getHandshake,
    std::function<std::error_code(std::string)> onHandshakeResponse,
    std::function<void(std::error_code)> onHandshakeDone)
{
    return std::make_unique<PersistentConnection>(std::move(host), port,
        context, std::move(onMessage), std::move(onReady),
        std::move(getHandshake), std::move(onHandshakeResponse),
        std::move(onHandshakeDone));
}

} // namespace communication
} // namespace one
