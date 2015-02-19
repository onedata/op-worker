/**
 * @file tcp_server.h
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef TCP_SERVER_H
#define TCP_SERVER_H

#include "server.h"

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

namespace one {
namespace proxy {

class tcp2tls_session;

/**
 * The tcp_server classr epresents single proxy server instance accepting TCP
 * connections.
 */
class tcp_server : public server,
                   public std::enable_shared_from_this<tcp_server> {
public:
    /**
     * @copydoc server::server
     * @param cert_path Path to proxy's certificate chain used on server
     * endpoint
     */
    tcp_server(boost::asio::io_service::strand &client_strand,
               boost::asio::io_service::strand &proxy_strand, int verify_type,
               const std::string &cert_path, uint16_t server_port,
               std::vector<std::string> ca_crl_paths);

    /**
     * Starts the proxy server.
     */
    virtual void start_accept() override;

private:
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::ssl::context context_;
};

} // namespace proxy
} // namespace one

#endif // TCP_SERVER_H
