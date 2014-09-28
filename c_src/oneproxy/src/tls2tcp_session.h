/*********************************************************************
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/


#ifndef TLS2TCP_SESSION_H
#define TLS2TCP_SESSION_H

#include "tls2tcp_session.h"

#include <vector>
#include <string>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio/buffer.hpp>

namespace one {
namespace proxy {

constexpr const char *INTERNAL_HEADER_PREFIX = "onedata-internal-";
constexpr const size_t SESSION_ID_SIZE = 16; // in bytes

class tls_server;

typedef boost::asio::ip::tcp::socket tcp_socket;
typedef boost::asio::ssl::stream<tcp_socket> ssl_socket;

/**
 * The tls2tcp_session class.
 * tls2tcp_session object represents single proxy connection between client and provider
 */
class tls2tcp_session : public std::enable_shared_from_this<tls2tcp_session> {
public:

    /**
     * tls2tcp_session constructor.
     * Constructs connection state but does not initialize TLS handshake.
     * @param server pointer to server object
     * @param client_io_service boost::asio::io_service used for client TLS connection
     * @param proxy_io_service boost::asio::io_service used for provider TCP connection
     * @param context Client's TLS context
     * @param forward_host Privider's hostname
     * @param forward_port Provider's port
     */
    tls2tcp_session(std::weak_ptr<tls_server> server,
                    boost::asio::io_service &client_io_service,
                    boost::asio::io_service &proxy_io_service,
                    boost::asio::ssl::context &context,
                    std::string forward_host,
                    std::string forward_port);

    ~tls2tcp_session();

    /// Returns client's TCP socket
    ssl_socket::lowest_layer_type &socket();

    /// Returns client's TLS socket
    ssl_socket &sslsocket();

    /**
     * Starts connection (initializes async TLS handshake with client).
     */
    void start();



private:
    static constexpr size_t buffer_size = 1024 * 1024;

    std::weak_ptr<tls_server> server_;

    ssl_socket client_socket_;
    tcp_socket proxy_socket_;
    boost::asio::io_service &proxy_io_service_;

    const std::string forward_host_;
    const std::string forward_port_;

    std::array<char, buffer_size> client_data_;
    std::array<char, buffer_size> proxy_data_;

    std::vector<std::string> client_cert_chain_;

    X509 *peer_cert_;

    std::string session_id_;
    mutable std::mutex socket_mutex_;

    std::string gen_session_data();


    /**
     * TLS handshake callback.
     * Closes connection on error, otherwise initializes proxy connection to provider.
     * @param error Boost error code
     */
    void handle_handshake(const boost::system::error_code &error);

    /// Handle data send by client.
    void handle_client_read(const boost::system::error_code &error,
                            size_t bytes_transferred);

    /// Handle data send by provider.
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
     * This callback accepts all certs while accumulating them in the object's state, so that
     * it's possible to verify peer later on.
     * @param preverified Tells wheter OpenSSL's internal validation was successful
     * @param ctx OpenSSL's verify context
     * @return true if peer cert shall be accepted, false otherwise
     */
    bool handle_verify_certificate(bool preverified,
                                   boost::asio::ssl::verify_context &ctx);

    /**
     * Verify client's certificate chain using GSI-based verification.
     * Client's certificates used for verification have to be present in connection state (see tls2tcp_session::handle_verify_certificate).
     * @return Verification status.
     */
    bool do_verify_peer();
};

} // namespace proxy
} // namespace one

#endif // TLS2TCP_SESSION_H
