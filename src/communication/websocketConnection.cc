/**
 * @file websocketConnection.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/websocketConnection.h"

namespace veil
{
namespace communication
{

WebsocketConnection::WebsocketConnection(std::shared_ptr<Mailbox> mailbox,
                                         endpoint_type &endpoint,
                                         const std::string &uri)
    : Connection{std::move(mailbox)}
{
    using websocketpp::lib::bind;
    namespace p = websocketpp::lib::placeholders;

    websocketpp::lib::error_code ec;
    m_connection = endpoint.get_connection(uri, ec);
    if(ec)
        return; // TODO

    m_connection->set_tls_init_handler(       bind(&WebsocketConnection::onTLSInit, this));
    m_connection->set_socket_init_handler(    bind(&WebsocketConnection::onSocketInit, this, p::_2));
    m_connection->set_message_handler(        bind(&WebsocketConnection::onMessage, this, p::_2));
    m_connection->set_open_handler(           bind(&WebsocketConnection::onOpen, this));
    m_connection->set_close_handler(          bind(&WebsocketConnection::onClose, this));
    m_connection->set_fail_handler(           bind(&WebsocketConnection::onFail, this));
    m_connection->set_ping_handler(           bind(&WebsocketConnection::onPing, this, p::_2));
    m_connection->set_pong_handler(           bind(&WebsocketConnection::onPong, this, p::_2));
    m_connection->set_pong_timeout_handler(   bind(&WebsocketConnection::onPongTimeout, this, p::_2));
    m_connection->set_interrupt_handler(      bind(&WebsocketConnection::onInterrupt, this));

    endpoint.connect(m_connection); // TODO: waiting or whatever
}

void WebsocketConnection::send()
{
}

WebsocketConnection::context_ptr WebsocketConnection::onTLSInit()
{

}

void WebsocketConnection::onSocketInit(WebsocketConnection::socket_type &socket)
{

}

void WebsocketConnection::onMessage(WebsocketConnection::message_ptr msg)
{

}

void WebsocketConnection::onOpen()
{

}

void WebsocketConnection::onClose()
{

}

void WebsocketConnection::onFail()
{

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

}

} // namespace communication
} // namespace veil
