/**
 * @file session.h
 * @author Rafal Slota
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef SESSION_H
#define SESSION_H

#include <memory>
#include <mutex>
#include <vector>
#include <string>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/buffer.hpp>

namespace one {
namespace proxy {

constexpr const char *INTERNAL_HEADER_PREFIX = "onedata-internal-";
constexpr const size_t SESSION_ID_SIZE = 64; // in bytes

class server;
class tls2tcp_session;
class tcp2tls_session;

using tcp_socket = boost::asio::ip::tcp::socket;
using ssl_socket = boost::asio::ssl::stream<tcp_socket>;

/**
 * The @c session class is a base class for classes representing a single
 * session.
 * Methods of @c session read data from the client socket and pass it to the
 * proxy socket.
 * @c derived is the class inheriting from the @c session.
 * @c client_socket_t is the type of the client socket.
 * @c proxy_socket_t is the type of the proxy socket.
 */
template <class derived, class client_socket_t, class proxy_socket_t>
class session : public std::enable_shared_from_this<derived> {
public:
    /**
     * @returns client-side socket.
     */
    typename client_socket_t::lowest_layer_type &client_socket();

    /**
     * @returns proxy-side socket.
     */
    typename proxy_socket_t::lowest_layer_type &proxy_socket();

protected:
    /**
     * Constructs connection state.
     * @param server pointer to server object
     * @param client_io_service boost::asio::io_service used for client TCP
     * connection
     * @param proxy_io_service boost::asio::io_service used for provider TLS
     * connection
     * @param context Client's TLS context
     */
    session(std::weak_ptr<server> server,
            boost::asio::io_service::strand &client_strand,
            boost::asio::io_service::strand &proxy_strand,
            boost::asio::ssl::context &context);

    /**
     * Destructor.
     * Frees allocated resources.
     */
    virtual ~session();

    /**
     * Handle data send by client.
     * @param error Boost error code
     * @param bytes_transferred Size of read data
     */
    void handle_client_read(const boost::system::error_code &error,
                            size_t bytes_transferred);

    /**
     * Handle data send by provider.
     * @param error Boost error code
     * @param bytes_transferred Size of read data
     */
    void handle_proxy_read(const boost::system::error_code &error,
                           size_t bytes_transferred);

    /**
     * Handle data write (to client) confirmation.
     * Schedules next provider's data read.
     * @param error Boost error code
     */
    void handle_client_write(const boost::system::error_code &error);

    /**
     * Handle data write (to provider) confirmation.
     * Schedules next client's data read.
     * @param error Boost error code
     */
    void handle_proxy_write(const boost::system::error_code &error);

    /**
     * Generic error handler for read/write/handshake operations.
     * Closes all sockects and cleanups state.
     * @param method Name of method that has failed. Only used for log message.
     * @param error Boost error code
     */
    void handle_error(const std::string &method,
                      const boost::system::error_code &error);

    /**
     * OpenSSL's cert verify callback.
     * This callback accepts all certs while accumulating them in the object's
     * state, so that
     * it's possible to verify peer later on.
     * @param preverified Tells wheter OpenSSL's internal validation was
     * successful
     * @param ctx OpenSSL's verify context
     * @return true if peer cert shall be accepted, false otherwise
     */
    bool handle_verify_certificate(bool preverified,
                                   boost::asio::ssl::verify_context &ctx);

    /**
     * Verify client's certificate chain using GSI-based verification.
     * Client's certificates used for verification have to be present in
     * connection state (see tcp2tls_session::handle_verify_certificate).
     * @return Verification status.
     */
    bool do_verify_peer();

    static constexpr size_t buffer_size = 1024 * 1024;

    client_socket_t client_socket_;
    proxy_socket_t proxy_socket_;
    boost::asio::io_service::strand &client_strand_;
    boost::asio::io_service::strand &proxy_strand_;
    boost::asio::io_service &proxy_io_service_;

    std::array<char, buffer_size> client_data_;
    std::array<char, buffer_size> proxy_data_;

    std::weak_ptr<server> server_;
    std::string session_id_;
    std::vector<std::string> cert_chain_;
    X509 *peer_cert_ = nullptr;

private:
    mutable std::mutex socket_mutex_;
};

template <>
session<tls2tcp_session, ssl_socket, tcp_socket>::session(
    std::weak_ptr<server> s, boost::asio::io_service::strand &client_strand,
    boost::asio::io_service::strand &proxy_strand,
    boost::asio::ssl::context &context);

template <>
session<tcp2tls_session, tcp_socket, ssl_socket>::session(
    std::weak_ptr<server> s, boost::asio::io_service::strand &client_strand,
    boost::asio::io_service::strand &proxy_strand,
    boost::asio::ssl::context &context);

extern template class session<tcp2tls_session, tcp_socket, ssl_socket>;
extern template class session<tls2tcp_session, ssl_socket, tcp_socket>;

} // namespace one
} // namespace proxy

#endif // SESSION_H
