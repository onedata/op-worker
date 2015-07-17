/**
 * @file persistentConnection.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_CONNECTION_H
#define HELPERS_COMMUNICATION_CONNECTION_H

#include "tlsApplication.hpp"
#include "tlsSocket.hpp"

#include "asio/buffer.hpp"
#include "asio/ssl/context.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <system_error>

namespace one {
namespace communication {

static constexpr std::chrono::seconds RECREATE_DELAY{2};
static constexpr std::chrono::seconds SHUTDOWN_TIMEOUT{5};

/**
 * @c Connection class represents a single TCP/TLS connection between the
 * client and a remote endpoint.
 * The Connection uses @c shared_from_this() to hold a temporary ownership of
 * itself; i.e. it ensures that the object will continue to exist while
 * a callback is in progress or any operations are scheduled on the object.
 *
 * On destruction, parent object is expected to call @c close() and reset
 * @c io_service associated with the object. All scheduled callbacks are dropped
 * along with associated shared pointers when the service is reset. If a
 * callback was already in progress, it won't be able to schedule new socket
 * operations as the socket is closed, and thus all shared pointers will
 * eventually be dropped.
 *
 * A @c Connection object can optionally send a custom handshake message
 * immediately after establishing a secure connection to the server. The object
 * then waits for a handshake response (and passes it up to
 * @c onHandshakeResponse callback) before indicating that it is ready to send
 * data. The @c onHandshakeResponse callback can decide to close the connection
 * by returning false.
 *
 * Aside from handshake-related callbacks, there are three main events that
 * a @c Connection instance can emit: @c onReady, @c onClosed and
 * @c onMessageReceived.
 * - @c onReady indicates that the @c Connection object is ready to send next
 *   message. Sending message before @c onReady is emitted is illegal, as is
 *   sending multiple messages after receiving a single @c onReady.
 * - @c onClosed is emitted when the connection has been closed, whether as
 *   a result of an error or through normal operation. The parent is expected
 *   to drop any references to the @c Connection instance, as it's no longer
 *   in a valid state.
 * - @c onMessageReceived is emitted when a message from the remote endpoint has
 *   been received.
 *
 * No callback shall perform a blocking or computationally-heavy operation, as
 * those would take up resources dedicated to managing connections.
 */
class PersistentConnection {
public:
    using Callback = std::function<void(const std::error_code &)>;

    /**
     * Constructor.
     * @param context asio SSL context used to establish a secure
     * connection with the server.
     */
    PersistentConnection(std::string host, const unsigned short port,
        asio::ssl::context &context, std::function<void(std::string)> onMessage,
        std::function<void(PersistentConnection &)> onReady,
        std::function<std::string()> getHandshake = {},
        std::function<std::error_code(std::string)> onHandshakeResponse = {},
        std::function<void(std::error_code)> onHandshakeDone = {});

    /**
     * Destructor.
     * Attempts to gracefully close the managed connection. The connection is
     * given @c SHUTDOWN_TIMEOUT timeout after which it's forcefully shut down.
     */
    virtual ~PersistentConnection();

    virtual void send(std::string message, Callback callback);
    virtual void connect();

    PersistentConnection(const PersistentConnection &) = delete;
    PersistentConnection(PersistentConnection &&) = delete;
    PersistentConnection &operator=(const PersistentConnection &) = delete;
    PersistentConnection &operator=(PersistentConnection &&) = delete;

private:
    void onConnect();
    void onHandshakeSent();
    void onHandshakeReceived();
    void onSent();
    void onError(const std::error_code &ec);
    void readLoop();

    void close();
    void notify(const std::error_code &ec = {});
    void start();

    template <typename... Args, typename SF>
    etls::Callback<Args...> createCallback(SF &&onSuccess);
    template <typename SF> void asyncRead(SF &&onSuccess);
    std::array<asio::const_buffer, 2> prepareOutBuffer(std::string message);
    asio::mutable_buffers_1 headerToBuffer(std::uint32_t &header);

    std::string m_host;
    const unsigned short m_port;
    asio::ssl::context &m_context;
    std::function<void(std::string)> m_onMessage;
    std::function<void(PersistentConnection &)> m_onReady;
    std::function<std::string()> m_getHandshake;
    std::function<std::error_code(std::string)> m_onHandshakeResponse;
    std::function<void(std::error_code)> m_onHandshakeDone;

    Callback m_callback;

    etls::TLSSocket::Ptr m_socket;
    etls::TLSApplication m_app{1};
    bool m_connected = false;
    int m_connectionId = 0;

    std::uint32_t m_inHeader;
    std::string m_inData;
    std::uint32_t m_outHeader;
    std::string m_outData;
};

std::unique_ptr<PersistentConnection> createConnection(std::string host,
    const unsigned short port, asio::ssl::context &context,
    std::function<void(std::string)> onMessage,
    std::function<void(PersistentConnection &)> onReady,
    std::function<std::string()> getHandshake = {},
    std::function<std::error_code(std::string)> onHandshakeResponse = {},
    std::function<void(std::error_code)> onHandshakeDone = {});

} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_CONNECTION_H
