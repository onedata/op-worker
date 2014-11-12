/**
 * @file tls2tcp_session.h
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef TLS2TCP_SESSION_H
#define TLS2TCP_SESSION_H

#include "session.h"

#include <vector>
#include <string>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/buffer.hpp>

namespace one {
namespace proxy {

class server;

/**
 * The tls2tcp_session class.
 * tls2tcp_session object represents single proxy connection between client and
 * provider
 */
class tls2tcp_session
    : public session<tls2tcp_session, ssl_socket, tcp_socket> {
public:
    /**
     * @copydoc session::session
     * @param forward_host Provider's hostname
     * @param forward_port Provider's port
     */
    tls2tcp_session(std::weak_ptr<server> server,
                    boost::asio::io_service &client_io_service,
                    boost::asio::io_service &proxy_io_service,
                    boost::asio::ssl::context &context,
                    std::string forward_host, std::string forward_port);

    virtual ~tls2tcp_session() = default;

    /**
     * Starts connection.
     * Initializes async TLS handshake with client.
     */
    void start();

protected:
    /**
     * Starts reading from sockets, deferring controll to methods from @c
     * session.
     * @param verified The result of OpenSSL peer certificate verification.
     */
    virtual void start_reading(bool verified);

private:
    std::string gen_session_data();

    /**
     * TLS handshake callback.
     * Closes connection on error, otherwise initializes proxy connection to
     * provider.
     * @param error Boost error code
     */
    void handle_handshake(const boost::system::error_code &error);

    const std::string forward_host_;
    const std::string forward_port_;
};

} // namespace proxy
} // namespace one

#endif // TLS2TCP_SESSION_H
