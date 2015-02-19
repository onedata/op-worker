/**
 * @file tls_server.h
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef TLS_SERVER_H
#define TLS_SERVER_H

#include "server.h"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

namespace one {
namespace proxy {

class tls2tcp_session;
class tls2tcp_http_session;

/**
 * The tls_server class represents single proxy server instance accepting TLS
 * connections.
 */
template <class session_t>
class tls_server : public server,
                   public std::enable_shared_from_this<tls_server<session_t>> {
public:
    /**
     * @copydoc server::server
     * @param cert_path Path to proxy's certificate chain used on server
     * endpoint
     * @param forward_host Host to which data shall be redirected
     * @param forward_port Port to which data shall be redirected
     */
    tls_server(boost::asio::io_service::strand &client_strand,
               boost::asio::io_service::strand &proxy_strand, int verify_type,
               const std::string &cert_path, uint16_t server_port,
               std::string forward_host, uint16_t forward_port,
               std::vector<std::string> ca_crl_paths);

    /**
     * Starts proxy server.
     */
    void start_accept();

private:
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::ssl::context context_;

    const std::string forward_host_;
    const std::string forward_port_;
};

extern template class tls_server<tls2tcp_session>;
extern template class tls_server<tls2tcp_http_session>;

} // namespace proxy
} // namespace one

#endif // TLS_SERVER_H
