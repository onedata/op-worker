/**
 * @file session.cpp
 * @author Rafal Slota
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#include "session.h"

#include "base64.h"
#include "log_message.h"
#include "server.h"
#include "tcp2tls_session.h"
#include "tls2tcp_session.h"

extern "C" {
#include "gpv/grid_proxy_verify.h"
}

#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/rand.h>

#include <vector>

namespace one {
namespace proxy {

namespace {
std::string gen_session_id()
{
    std::vector<unsigned char> session_id_bin(SESSION_ID_SIZE, 0);
    if (RAND_bytes(session_id_bin.data(), session_id_bin.size()))
        return utils::base64_encode(session_id_bin);

    LOG(ERROR) << "Could not generate session id";
    throw std::runtime_error("Could not generate session id");
}
}

template <>
session<tls2tcp_session, ssl_socket, tcp_socket>::session(
    std::weak_ptr<server> s, boost::asio::io_service &client_io_service,
    boost::asio::io_service &proxy_io_service,
    boost::asio::ssl::context &context)
    : client_socket_(client_io_service, context)
    , proxy_socket_(proxy_io_service)
    , proxy_io_service_(proxy_io_service)
    , server_(s)
    , session_id_(gen_session_id())
{
}

template <>
session<tcp2tls_session, tcp_socket, ssl_socket>::session(
    std::weak_ptr<server> s, boost::asio::io_service &client_io_service,
    boost::asio::io_service &proxy_io_service,
    boost::asio::ssl::context &context)
    : client_socket_(client_io_service)
    , proxy_socket_(proxy_io_service, context)
    , proxy_io_service_(proxy_io_service)
    , server_(s)
    , session_id_(gen_session_id())
{
}

template <class deriv, class csock, class psock>
session<deriv, csock, psock>::~session()
{
    if (peer_cert_)
        X509_free(peer_cert_);

    if (auto s = server_.lock())
        s->remove_session(session_id_);
}

template <class deriv, class csock, class psock>
typename csock::lowest_layer_type &session<deriv, csock, psock>::client_socket()
{
    return client_socket_.lowest_layer();
}

template <class deriv, class csock, class psock>
typename psock::lowest_layer_type &session<deriv, csock, psock>::proxy_socket()
{
    return proxy_socket_.lowest_layer();
}

template <class deriv, class csock, class psock>
bool session<deriv, csock, psock>::handle_verify_certificate(
    bool preverified, boost::asio::ssl::verify_context &ctx)
{
    char *x509_buf = nullptr;
    X509 *cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
    size_t der_size =
        i2d_X509(cert, reinterpret_cast<unsigned char **>(&x509_buf));

    std::string der_cert(x509_buf, der_size);
    delete x509_buf;

    cert_chain_.push_back(der_cert);

#ifndef NDEBUG
    std::array<char, 256> subject_name;
    X509_NAME_oneline(X509_get_subject_name(cert), subject_name.data(),
                      subject_name.size());
    LOG(DEBUG) << "Verifying " << subject_name.data();
#endif

    return true;
}

template <class deriv, class csock, class psock>
bool session<deriv, csock, psock>::do_verify_peer()
{
    if (cert_chain_.size() > 0) {
        auto server = server_.lock();
        if (!server)
            return false;

        auto &crls = server->get_crl();
        auto &cas = server->get_ca();

        GPV_CTX ctx;

        // Init gpv_ctx
        if (gpv_init(&ctx) != GPV_SUCCESS) {
            LOG(ERROR) << "Cannot initialize GPV CTX";
            return false;
        }

        if (gpv_set_leaf_cert(&ctx, cert_chain_.back().data(),
                              cert_chain_.back().size()) != GPV_SUCCESS) {
            LOG(ERROR) << "Cannot setup peer cert for GPV CTX";
            return false;
        }

        for (auto &cert : cert_chain_) {
            gpv_add_chain_cert(&ctx, cert.data(), cert.size());
        }

        for (auto &crl : crls) {
            gpv_add_crl_cert(&ctx, crl.data(), crl.size());
        }

        for (auto &ca : cas) {
            gpv_add_trusted_ca(&ctx, ca.data(), ca.size());
        }

        auto result = gpv_verify(&ctx);
        auto error = gpv_get_error(&ctx);

        gpv_cleanup(&ctx);

        X509 *peer_cert = nullptr;
        auto p =
            reinterpret_cast<const unsigned char *>(cert_chain_.back().data());
        if (!d2i_X509(&peer_cert, &p, cert_chain_.back().size())) {
            LOG(ERROR) << "Could not convert peer cert to internal format";
            return false;
        }

        if (result == GPV_SUCCESS) {
            peer_cert_ = peer_cert;
            return true;
        } else {
            std::array<char, 256> subject_name;
            X509_NAME_oneline(X509_get_subject_name(peer_cert),
                              subject_name.data(), subject_name.size());
            LOG(INFO) << "Peer " << subject_name.data()
                      << " rejected due to SSL error code: {" << result << ", "
                      << error << "}";
            X509_free(peer_cert);
        }
    }

    return false;
}

template <class deriv, class csock, class psock>
void session<deriv, csock, psock>::handle_client_read(
    const boost::system::error_code &error, size_t bytes_transferred)
{
    if (!error) {
        boost::asio::async_write(
            proxy_socket_, boost::asio::const_buffers_1(client_data_.data(),
                                                        bytes_transferred),
            std::bind(&session<deriv, csock, psock>::handle_proxy_write,
                      static_cast<deriv *>(this)->shared_from_this(),
                      std::placeholders::_1));
    } else {
        handle_error(__func__, error);
    }
}

template <class deriv, class csock, class psock>
void session<deriv, csock, psock>::handle_proxy_read(
    const boost::system::error_code &error, size_t bytes_transferred)
{
    if (!error) {
        boost::asio::async_write(
            client_socket_,
            boost::asio::const_buffers_1(proxy_data_.data(), bytes_transferred),
            std::bind(&session<deriv, csock, psock>::handle_client_write,
                      static_cast<deriv *>(this)->shared_from_this(),
                      std::placeholders::_1));
    } else {
        handle_error(__func__, error);
    }
}

template <class deriv, class csock, class psock>
void session<deriv, csock, psock>::handle_client_write(
    const boost::system::error_code &error)
{
    if (!error) {
        proxy_socket_.async_read_some(
            boost::asio::mutable_buffers_1(proxy_data_.data(),
                                           proxy_data_.size()),
            std::bind(&session<deriv, csock, psock>::handle_proxy_read,
                      static_cast<deriv *>(this)->shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2));
    } else {
        handle_error(__func__, error);
    }
}

template <class deriv, class csock, class psock>
void session<deriv, csock, psock>::handle_proxy_write(
    const boost::system::error_code &error)
{
    if (!error) {
        client_socket_.async_read_some(
            boost::asio::mutable_buffers_1(client_data_.data(),
                                           client_data_.size()),
            std::bind(&session<deriv, csock, psock>::handle_client_read,
                      static_cast<deriv *>(this)->shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2));
    } else {
        handle_error(__func__, error);
    }
}

template <class deriv, class csock, class psock>
void session<deriv, csock, psock>::handle_error(
    const std::string &method, const boost::system::error_code &error)
{
    using namespace boost::asio::error;

    if (error == eof) {
        LOG(DEBUG) << "Connection closed...";
    } else if (error == operation_aborted) {
        LOG(DEBUG) << "Connection aborted...";
    } else {
        LOG(ERROR) << method << " failed due to: " << error.message();
    }

    try {
        std::lock_guard<std::mutex> guard(socket_mutex_);
        if (proxy_socket().is_open())
            proxy_socket().close();
        if (client_socket().is_open())
            client_socket().close();
    }
    catch (boost::system::error_code &e) {
        if (e == bad_descriptor) {
            LOG(DEBUG) << "Cannot close socket because it is already closed";
        } else {
            LOG(WARNING) << "Cannot close socket due to: " << e.message();
        }
    }
    catch (std::exception &e) {
        LOG(DEBUG) << "Unknown session error " << e.what();
    }
}

template class session<tcp2tls_session, tcp_socket, ssl_socket>;
template class session<tls2tcp_session, ssl_socket, tcp_socket>;

} // namespace one
} // namespace proxy
