/**
 * @file tls_server.cpp
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#include "tls_server.h"

#include "log_message.h"
#include "tls2tcp_session.h"
#include "tls2tcp_http_session.h"

#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <fstream>

namespace one {
namespace proxy {

template <class session_t>
tls_server<session_t>::tls_server(boost::asio::io_service &io_service,
                                  boost::asio::io_service::strand &strand,
                                  int verify_type, const std::string &cert_path,
                                  uint16_t server_port,
                                  std::string forward_host,
                                  uint16_t forward_port,
                                  std::vector<std::string> ca_crl_dirs)
    : server(io_service, strand, verify_type,
             std::move(ca_crl_dirs))
    , acceptor_(io_service, boost::asio::ip::tcp::endpoint(
                                       boost::asio::ip::tcp::v4(), server_port))
    , context_(boost::asio::ssl::context::sslv23_server)
    , forward_host_(std::move(forward_host))
    , forward_port_(std::to_string(forward_port))
{
    init_server(acceptor_, context_, cert_path);
}

template <class session_t> void tls_server<session_t>::start_accept()
{
    auto new_session = std::make_shared<session_t>(
        tls_server<session_t>::shared_from_this(), io_service_,
            strand_, context_, forward_host_, forward_port_);

    acceptor_.async_accept(
        new_session->client_socket(),
        std::bind(&tls_server<session_t>::template handle_accept<session_t>,
                  tls_server<session_t>::shared_from_this(), new_session,
                  std::placeholders::_1));
}

template class tls_server<tls2tcp_session>;
template class tls_server<tls2tcp_http_session>;

} // namespace proxy
} // namespace one
