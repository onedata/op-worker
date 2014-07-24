/**
 * @file websocketConnectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/websocketConnectionPool.h"

#include "communication/websocketConnection.h"
#include "logging.h"
#include "make_unique.h"

#include <chrono>
#include <functional>
#include <future>

template class websocketpp::client<websocketpp::config::asio_tls_client>;

namespace veil
{
namespace communication
{

class CertificateData;

WebsocketConnectionPool::WebsocketConnectionPool(const unsigned int connectionsNumber,
                                                 std::string uri,
                                                 std::shared_ptr<const CertificateData> certificateData,
                                                 const bool verifyServerCertificate)
    : ConnectionPool{connectionsNumber, std::move(uri)}
    , m_certificateData{std::move(certificateData)}
    , m_verifyServerCertificate{verifyServerCertificate}
{
    using websocketpp::lib::bind;
    namespace p = websocketpp::lib::placeholders;

#ifndef NDEBUG
    m_endpoint.clear_access_channels(websocketpp::log::alevel::all);
    m_endpoint.clear_error_channels(websocketpp::log::elevel::all);
#endif

    m_endpoint.init_asio();
    m_endpoint.start_perpetual();

    m_ioThread = std::thread{&endpoint_type::run, &m_endpoint};

    m_endpoint.set_tls_init_handler   (bind(&WebsocketConnectionPool::onTLSInit, this));
    m_endpoint.set_socket_init_handler(bind(&WebsocketConnectionPool::onSocketInit, this, p::_2));
}

WebsocketConnectionPool::~WebsocketConnectionPool()
{
    m_endpoint.stop_perpetual();
    close();
    m_ioThread.join();
}

WebsocketConnectionPool::context_ptr WebsocketConnectionPool::onTLSInit()
{
    try
    {
        auto ctx = std::make_shared<context_type>(boost::asio::ssl::context::sslv3);

        ctx->set_options(boost::asio::ssl::context::default_workarounds |
                         boost::asio::ssl::context::no_sslv2 |
                         boost::asio::ssl::context::single_dh_use);

        ctx->set_default_verify_paths();
        ctx->set_verify_mode(m_verifyServerCertificate
                             ? boost::asio::ssl::verify_peer
                             : boost::asio::ssl::verify_none);

        SSL_CTX *ssl_ctx = ctx->native_handle();
        long mode = SSL_CTX_get_session_cache_mode(ssl_ctx);
        mode |= SSL_SESS_CACHE_CLIENT;
        SSL_CTX_set_session_cache_mode(ssl_ctx, mode);

        return m_certificateData->initContext(ctx);

    }
    catch(boost::system::system_error &e)
    {
        LOG(ERROR) << "Cannot initialize TLS socket due to: " << e.what();
    }

    return {};
}

void WebsocketConnectionPool::onSocketInit(socket_type &socket)
{
    socket.lowest_layer().set_option(boost::asio::ip::tcp::no_delay(true));
}

std::unique_ptr<Connection> WebsocketConnectionPool::createConnection()
{
    return std::make_unique<WebsocketConnection>(
                m_onMessageCallback,
                std::bind(&WebsocketConnectionPool::onFail, this, std::placeholders::_1),
                std::bind(&WebsocketConnectionPool::onOpen, this, std::placeholders::_1),
                std::bind(&WebsocketConnectionPool::onError, this, std::placeholders::_1),
                m_endpoint, m_uri, m_certificateData, m_verifyServerCertificate);
}

} // namespace communication
} // namespace veil
