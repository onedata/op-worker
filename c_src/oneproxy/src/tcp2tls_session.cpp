/**
 * @file tcp2tls_session.cpp
 * @author Rafal Slota
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#include "tcp2tls_session.h"

#include "log_message.h"
#include "tls_server.h"
#include "base64.h"
#include "oneproxy_messages.pb.h"

#include <boost/bind.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <string>

using std::string;
using namespace std::placeholders;

namespace one {
namespace proxy {

tcp2tls_session::tcp2tls_session(std::weak_ptr<server> s,
                                 boost::asio::io_service &client_io_service,
                                 boost::asio::io_service &proxy_io_service,
                                 boost::asio::ssl::context &context,
                                 boost::asio::ssl::verify_mode verify_mode)
    : session(std::move(s), client_io_service, proxy_io_service, context)
    , verify_mode_(verify_mode)
{
}

void tcp2tls_session::start()
{
    boost::asio::async_read(
        client_socket_,
        boost::asio::buffer(client_data_.data(), client_data_.size()),
        boost::asio::transfer_exactly(4),
        std::bind(&tcp2tls_session::proxy_preinit, shared_from_this(), _1, _2));
}

void tcp2tls_session::proxy_preinit(const boost::system::error_code &error,
                                    const std::size_t bytes_transferred)
{
    if (error) {
        handle_error(__func__, error);
        return;
    }

    size_t msgsize = 0;
    for (size_t i = 0; i < bytes_transferred; ++i)
        msgsize = (msgsize << 8) + client_data_[i];

    boost::asio::async_read(
        client_socket_,
        boost::asio::buffer(client_data_.data(), client_data_.size()),
        boost::asio::transfer_exactly(msgsize),
        std::bind(&tcp2tls_session::proxy_init, shared_from_this(), _1, _2));
}

void tcp2tls_session::proxy_init(const boost::system::error_code &op_error,
                                 const std::size_t bytes_transferred)
{
    if (op_error) {
        handle_error(__func__, op_error);
        return;
    }

    proto::ProxyInit msg;
    if (!msg.ParseFromArray(client_data_.data(), bytes_transferred)) {
        LOG(ERROR) << "Malformed ProxyInit message received.";
        return;
    }

    LOG(DEBUG) << "Proxying to " << msg.host() << ":" << msg.port();

    using boost::asio::ip::tcp;

    tcp::resolver resolver(proxy_io_service_);
    tcp::resolver::query query(msg.host(), msg.port());

    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
    tcp::resolver::iterator end;

    boost::system::error_code error = boost::asio::error::host_not_found;

    while (error && endpoint_iterator != end) {
        proxy_socket().close();
        proxy_socket().connect(*(endpoint_iterator++), error);
    }

    if (error) {
        LOG(ERROR) << "Proxy endpoint (" << msg.host() << ":" << msg.port()
                   << ") unreachable: " << error.message();
        return;
    }

    client_socket_.set_option(boost::asio::ip::tcp::no_delay(true));
    proxy_socket_.lowest_layer().set_option(
        boost::asio::ip::tcp::no_delay(true));

    proxy_socket_.set_verify_callback(
        std::bind(&tcp2tls_session::handle_verify_certificate, this,
                  std::placeholders::_1, std::placeholders::_2));

    cert_chain_.clear();
    proxy_socket_.async_handshake(boost::asio::ssl::stream_base::client,
                                  std::bind(&tcp2tls_session::handle_handshake,
                                            shared_from_this(),
                                            std::placeholders::_1));
}

void tcp2tls_session::handle_handshake(const boost::system::error_code &error)
{
    if (error) {
        handle_error(__func__, error);
        return;
    }

    try {
        if (verify_mode_ == boost::asio::ssl::verify_peer &&
            !do_verify_peer()) {
            LOG(ERROR) << "Peer verification error";
            return;
        }

        // Start reading...
        proxy_socket_.async_read_some(
            boost::asio::buffer(proxy_data_.data(), proxy_data_.size()),
            std::bind(&tcp2tls_session::handle_proxy_read, shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2));

        client_socket_.async_read_some(
            boost::asio::buffer(client_data_.data(), client_data_.size()),
            std::bind(&tcp2tls_session::handle_client_read, shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2));
    }
    catch (boost::system::error_code &e) {
        LOG(ERROR) << "Cannot initialize proxy connection due to: "
                   << e.message();
    }
    catch (boost::exception &e) {
        LOG(DEBUG) << "Unknown handshake error";
    }
}

} // namespace proxy
} // namespace one
