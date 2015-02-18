/**
 * @file tcp_server.cpp
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#include "tcp_server.h"

#include "log_message.h"
#include "tcp2tls_session.h"

#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <fstream>

namespace one {
namespace proxy {

tcp_server::tcp_server(boost::asio::io_service &client_io_service,
                       boost::asio::io_service &proxy_io_service,
                       int verify_type, const std::string &cert_path,
                       uint16_t server_port,
                       std::vector<std::string> ca_crl_dirs)
    : server(client_io_service, proxy_io_service, verify_type,
             std::move(ca_crl_dirs))
    , acceptor_(
          client_io_service,
          boost::asio::ip::tcp::endpoint(
              boost::asio::ip::address::from_string("127.0.0.1"), server_port))
    , context_(boost::asio::ssl::context::sslv23_client)
{
    init_server(acceptor_, context_, cert_path);
}

void tcp_server::start_accept()
{
    auto new_session = std::make_shared<tcp2tls_session>(
        shared_from_this(), client_io_service_, proxy_io_service_, context_,
        verify_type_);

    acceptor_.async_accept(
        new_session->client_socket(),
        std::bind(&tcp_server::handle_accept<tcp2tls_session>,
                  shared_from_this(), new_session, std::placeholders::_1));
}

} // namespace proxy
} // namespace one
