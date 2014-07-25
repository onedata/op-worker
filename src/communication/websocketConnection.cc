/**
 * @file websocketConnection.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/websocketConnection.h"

#include "communication/certificateData.h"
#include "communication/exception.h"
#include "logging.h"

namespace veil
{
namespace communication
{

WebsocketConnection::WebsocketConnection(std::function<void(const std::string&)> onMessageCallback,
                                         std::function<void(Connection&, std::exception_ptr)> onFailCallback,
                                         std::function<void(Connection&)> onOpenCallback,
                                         std::function<void(Connection&)> onErrorCallback,
                                         endpoint_type &endpoint,
                                         const std::string &uri,
                                         std::shared_ptr<const CertificateData> certificateData,
                                         const bool verifyServerCertificate)
    : Connection{std::move(onMessageCallback), std::move(onFailCallback), std::move(onOpenCallback), std::move(onErrorCallback)}
    , m_endpoint(endpoint)
    , m_certificateData{std::move(certificateData)}
    , m_verifyServerCertificate{verifyServerCertificate}
{
    using websocketpp::lib::bind;
    namespace p = websocketpp::lib::placeholders;

    websocketpp::lib::error_code ec;
    auto connection = endpoint.get_connection(uri, ec);
    if(ec)
        throw ConnectionError{"cannot connect to the endpoint '" + uri + "': " + ec.message()};

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

WebsocketConnection::~WebsocketConnection()
{
    m_onErrorCallback = []{};

    websocketpp::lib::error_code ec;
    m_endpoint.close(m_connection, websocketpp::close::status::going_away, "", ec);
}

void WebsocketConnection::send(const std::string &payload)
{
    const auto connection = m_endpoint.get_con_from_hdl(m_connection);
    if(!connection)
        throw SendError{"websocketConnection instance has no associated asio "
                        "connection."};

    websocketpp::lib::error_code ec;
    m_endpoint.send(connection, payload,
                    websocketpp::frame::opcode::binary, ec);

    if(ec)
        throw SendError{"error queuing message: " + ec.message()};
}

void WebsocketConnection::onMessage(message_ptr msg)
{
    m_onMessageCallback(msg->get_payload());
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
    try
    {
        const auto conn = m_endpoint.get_con_from_hdl(m_connection);
        if(conn)
        {
            const int verifyResult =
                SSL_get_verify_result(conn->get_socket().native_handle());

            if(verifyResult != 0 && m_verifyServerCertificate)
                throw InvalidServerCertificate{"Server certificate verification "
                                               "failed.  OpenSSL error: " +
                                               std::to_string(verifyResult)};
        }
    }
    catch(Exception&)
    {
        m_onFailCallback(std::current_exception());
        return;
    }

    m_onFailCallback({});
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
