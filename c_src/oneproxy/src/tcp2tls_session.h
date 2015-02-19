/**
 * @file tcp2tls_session.h
 * @author Rafal Slota
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef TCP2TLS_SESSION_H
#define TCP2TLS_SESSION_H

#include "session.h"

#include <vector>
#include <string>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/buffer.hpp>

namespace one {
namespace proxy {

class tcp_server;

/**
 * The tcp2tls_session class.
 * tcp2tls_session object represents single proxy connection between client and
 * provider, where the client connects through a tcp socket, and the connection
 * is then wrapped in ssl.
 */
class tcp2tls_session
    : public session<tcp2tls_session, tcp_socket, ssl_socket> {
public:
    /**
     * @copydoc session::session
     * @param verify_mode Verification mode of peer certificate.
     */
    tcp2tls_session(std::weak_ptr<server> server,
                    boost::asio::io_service::strand &client_strand,
                    boost::asio::io_service::strand &proxy_strand,
                    boost::asio::ssl::context &context,
                    boost::asio::ssl::verify_mode verify_mode);

    /**
     * Starts the connection.
     * Initiates a receive of a set amount of bytes, with @c proxy_preinit as
     * the callback.
     */
    void start();

private:
    /**
     * Reads a set amount of bytes to determine control message size from the
     * client.
     * @param error Boost error code
     * @param bytes_transferred The number of read bytes
     */
    void proxy_preinit(const boost::system::error_code &error,
                       const size_t bytes_transferred);

    /**
     * Reads a control message to the client and opens a socket to the
     * destination.
     * @param error Boost error code
     * @param bytes_transferred The number of read bytes
     */
    void proxy_init(const boost::system::error_code &error,
                    const size_t bytes_transferred);

    /**
     * TLS handshake callback.
     * Closes connection on error, otherwise initializes proxy connection to
     * provider.
     * @param error Boost error code
     */
    void handle_handshake(const boost::system::error_code &error);

    const boost::asio::ssl::verify_mode verify_mode_;
};

} // namespace proxy
} // namespace one

#endif // TCP2TLS_SESSION_H
