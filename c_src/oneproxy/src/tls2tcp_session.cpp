/**
 * @file tls2tcp_session.cpp
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#include "tls2tcp_session.h"

#include "log_message.h"
#include "tls_server.h"
#include "base64.h"

extern "C" {
#include "gpv/grid_proxy_verify.h"
}

#include <boost/bind.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <string>
#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/rand.h>

using std::string;

namespace one {
namespace proxy {

tls2tcp_session::tls2tcp_session(std::weak_ptr<server> s,
                                 boost::asio::io_service &client_io_service,
                                 boost::asio::io_service &proxy_io_service,
                                 boost::asio::ssl::context &context,
                                 std::string forward_host,
                                 std::string forward_port)
    : session(std::move(s), client_io_service, proxy_io_service, context)
    , forward_host_(std::move(forward_host))
    , forward_port_(std::move(forward_port))
{
}

std::string tls2tcp_session::gen_session_data()
{
    auto der_2_pem = [](const std::string &der) {
        std::vector<unsigned char> pem;

        X509 *peer_cert = nullptr;
        auto p = reinterpret_cast<const unsigned char *>(der.data());
        if (d2i_X509(&peer_cert, &p, der.size())) {
            BIO *mem = BIO_new(BIO_s_mem());
            auto buff = BUF_MEM_new();
            BIO_set_mem_buf(mem, buff, 0);
            if (PEM_write_bio_X509(mem, peer_cert)) {
                std::copy_n(buff->data, buff->length, std::back_inserter(pem));
            }

            BIO_free(mem);
        }

        return pem;
    };

    std::vector<unsigned char> pem_chain;
    for (auto &der : cert_chain_) {
        auto pem = der_2_pem(der);
        pem_chain.insert(pem_chain.end(), pem.begin(), pem.end());
    }

    return utils::base64_encode(pem_chain);
}

void tls2tcp_session::start()
{
    client_socket_.set_verify_callback(
        std::bind(&tls2tcp_session::handle_verify_certificate, this,
                  std::placeholders::_1, std::placeholders::_2));

    cert_chain_.clear();
    client_socket_.async_handshake(boost::asio::ssl::stream_base::server,
                                   std::bind(&tls2tcp_session::handle_handshake,
                                             shared_from_this(),
                                             std::placeholders::_1));
}

void tls2tcp_session::handle_handshake(const boost::system::error_code &error)
{
    if (!error) {
        try {
            auto verified = do_verify_peer();
            if (verified) {
                auto data = gen_session_data();
                auto server = server_.lock();
                if (server)
                    server->register_session(session_id_, data);
            }

            using boost::asio::ip::tcp;

            tcp::resolver resolver(proxy_io_service_);
            tcp::resolver::query query(forward_host_, forward_port_);

            tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
            tcp::resolver::iterator end;

            boost::system::error_code error =
                boost::asio::error::host_not_found;
            while (error && endpoint_iterator != end) {
                proxy_socket_.close();
                proxy_socket_.connect(*endpoint_iterator++, error);
            }

            if (error) {
                LOG(ERROR) << "Proxy endpoint (" << forward_host_ << ":"
                           << forward_port_
                           << ") unreachable: " << error.message();
                return;
            }

            // Setup sockets
            proxy_socket_.set_option(boost::asio::ip::tcp::no_delay(true));
            client_socket().set_option(boost::asio::ip::tcp::no_delay(true));

            // Start reading...
            start_reading(verified);
        }
        catch (boost::system::error_code &e) {
            LOG(ERROR) << "Cannot initialize proxy connection due to: "
                       << e.message();
        }
        catch (boost::exception &e) {
            LOG(DEBUG) << "Unknown handshake error";
        }

    } else {
        handle_error(__func__, error);
    }
}

void tls2tcp_session::start_reading(bool /*verified*/)
{
    proxy_socket_.async_read_some(
        boost::asio::buffer(proxy_data_.data(), proxy_data_.size()),
        std::bind(&tls2tcp_session::handle_proxy_read, shared_from_this(),
                  std::placeholders::_1, std::placeholders::_2));

    client_socket_.async_read_some(
        boost::asio::buffer(client_data_.data(), client_data_.size()),
        std::bind(&tls2tcp_session::handle_client_read, shared_from_this(),
                  std::placeholders::_1, std::placeholders::_2));
}

} // namespace proxy
} // namespace one
