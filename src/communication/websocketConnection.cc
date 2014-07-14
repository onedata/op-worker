/**
 * @file websocketConnection.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/websocketConnection.h"

#include "logging.h"
#include "communication/certificateData.h"
#include "communication/mailbox.h"

namespace veil
{
namespace communication
{

WebsocketConnection::WebsocketConnection(std::shared_ptr<Mailbox> mailbox,
                                         std::function<void(Connection&)> onFailCallback,
                                         std::function<void(Connection&)> onOpenCallback,
                                         std::function<void(Connection&)> onErrorCallback,
                                         endpoint_type &endpoint,
                                         const std::string &uri,
                                         std::shared_ptr<const CertificateData> certificateData,
                                         const bool verifyServerCertificate)
    : Connection{std::move(mailbox), onFailCallback, onOpenCallback, onErrorCallback}
    , m_endpoint(endpoint)
    , m_certificateData{std::move(certificateData)}
    , m_verifyServerCertificate{verifyServerCertificate}
{
    using websocketpp::lib::bind;
    namespace p = websocketpp::lib::placeholders;

    websocketpp::lib::error_code ec;
    auto connection = endpoint.get_connection(uri, ec);
    if(ec)
        return; // TODO

    connection->set_tls_init_handler    (bind(&WebsocketConnection::onTLSInit, this));
    connection->set_socket_init_handler (bind(&WebsocketConnection::onSocketInit, this, p::_2));
    connection->set_message_handler     (bind(&WebsocketConnection::onMessage, this, p::_2));
    connection->set_open_handler        (bind(&WebsocketConnection::onOpen, this));
    connection->set_close_handler       (bind(&WebsocketConnection::onClose, this));
    connection->set_fail_handler        (bind(&WebsocketConnection::onFail, this));
    connection->set_ping_handler        (bind(&WebsocketConnection::onPing, this, p::_2));
    connection->set_pong_handler        (bind(&WebsocketConnection::onPong, this, p::_2));
    connection->set_pong_timeout_handler(bind(&WebsocketConnection::onPongTimeout, this, p::_2));
    connection->set_interrupt_handler   (bind(&WebsocketConnection::onInterrupt, this));

    m_connection = connection;

    endpoint.connect(connection);
}

void WebsocketConnection::send(const std::string &payload)
{
    const auto connection = m_endpoint.get_con_from_hdl(m_connection);
    if(connection) // TODO
    {
        websocketpp::lib::error_code ec;
        m_endpoint.send(connection, payload,
                       websocketpp::frame::opcode::binary, ec);

        if(ec); // TODO: throw UNDERLYING_LIB_ERROR;
    }
}

void WebsocketConnection::close()
{
    Connection::close();
    websocketpp::lib::error_code ec;
    m_endpoint.close(m_connection, websocketpp::close::status::going_away, "");
}

WebsocketConnection::context_ptr WebsocketConnection::onTLSInit()
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

void WebsocketConnection::onSocketInit(socket_type &socket)
{
    socket.lowest_layer().set_option(boost::asio::ip::tcp::no_delay(true));
}

void WebsocketConnection::onMessage(message_ptr msg)
{
    m_mailbox->onMessage(msg->get_payload());
}

void WebsocketConnection::onOpen()
{
    m_onOpenCallback();
}

void WebsocketConnection::onClose()
{
    m_onErrorCallback();
}

void WebsocketConnection::onFail()
{
    m_onFailCallback();
}

bool WebsocketConnection::onPing(std::string)
{
    return true;
}

void WebsocketConnection::onPong(std::string)
{
}

void WebsocketConnection::onPongTimeout(std::string)
{
}

void WebsocketConnection::onInterrupt()
{
    m_onErrorCallback();
}

} // namespace communication
} // namespace veil
