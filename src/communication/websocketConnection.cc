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

namespace one
{
namespace communication
{

WebsocketConnection::WebsocketConnection(std::function<void(const std::string&)> onMessageCallback,
                                         std::function<void(Connection&, std::exception_ptr)> onFailCallback,
                                         std::function<void(Connection&)> onOpenCallback,
                                         std::function<void(Connection&)> onErrorCallback,
                                         endpoint_type &endpoint,
                                         const std::string &uri,
                                         const std::unordered_map<std::string, std::string> &additionalHeaders,
                                         std::shared_ptr<const CertificateData> certificateData,
                                         const bool verifyServerCertificate)
    : Connection{std::move(onMessageCallback), std::move(onFailCallback),
                 std::move(onOpenCallback), std::move(onErrorCallback)}
    , m_endpoint(endpoint)
    , m_certificateData{std::move(certificateData)}
    , m_verifyServerCertificate{verifyServerCertificate}
{
    using websocketpp::lib::bind;
    namespace p = websocketpp::lib::placeholders;

    LOG(INFO) << "Opening connection to '" << uri << "'";

    websocketpp::lib::error_code ec;
    auto connection = endpoint.get_connection(uri, ec);
    if(ec)
        throw ConnectionError{"cannot connect to the endpoint '" + uri + "': " +
                              ec.message()};

    connection->set_message_handler     (bind(&WebsocketConnection::onMessage, this, p::_2));
    connection->set_open_handler        (bind(&WebsocketConnection::onOpen, this));
    connection->set_close_handler       (bind(&WebsocketConnection::onClose, this));
    connection->set_fail_handler        (bind(&WebsocketConnection::onFail, this));
    connection->set_ping_handler        (bind(&WebsocketConnection::onPing, this, p::_2));
    connection->set_pong_handler        (bind(&WebsocketConnection::onPong, this, p::_2));
    connection->set_pong_timeout_handler(bind(&WebsocketConnection::onPongTimeout, this, p::_2));
    connection->set_interrupt_handler   (bind(&WebsocketConnection::onInterrupt, this));

    for(const auto &header: additionalHeaders)
        connection->append_header(header.first, header.second);

    m_connection = connection;

    endpoint.connect(connection);
}

WebsocketConnection::~WebsocketConnection()
{
    websocketpp::lib::error_code ec;
    const auto conn = m_endpoint.get_con_from_hdl(m_connection, ec);

    m_onErrorCallback = []{};

    if(ec)
        LOG(WARNING) << "Error retrieving connection pointer: " << ec.message();
    else if(!conn)
        LOG(WARNING) << "Connection has no associated ASIO connection.";
    else
    {
        conn->set_close_handler(nullptr);
        m_endpoint.close(m_connection, websocketpp::close::status::going_away, "", ec);
        if(ec)
            LOG(WARNING) << "Error closing connection: " << ec.message();
    }
}

void WebsocketConnection::send(const std::string &payload)
{
    websocketpp::lib::error_code ec;
    const auto conn = m_endpoint.get_con_from_hdl(m_connection, ec);

    if(ec)
        throw SendError{"error retrieving connection pointer: " + ec.message()};

    if(!conn)
        throw SendError{"websocketConnection instance has no associated ASIO "
                        "connection."};

    m_endpoint.send(conn, payload, websocketpp::frame::opcode::binary, ec);

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
    websocketpp::lib::error_code ec;
    const auto conn = m_endpoint.get_con_from_hdl(m_connection, ec);

    if(ec)
        LOG(WARNING) << "Error retrieving connection pointer: " << ec.message();
    else if(!conn)
        LOG(WARNING) << "Connection has no associated ASIO connection.";
    else
        LOG(WARNING) << "Connection closed: " << conn->get_ec().message();

    m_onErrorCallback();
}

void WebsocketConnection::onFail()
{
    try
    {
        websocketpp::lib::error_code ec;
        const auto conn = m_endpoint.get_con_from_hdl(m_connection, ec);

        if(ec)
        {
            LOG(WARNING) << "Error retrieving connection pointer: " << ec.message();
        }
        else if(!conn)
        {
            LOG(WARNING) << "Connection has no associated ASIO connection.";
        }
        else
        {
            const int verifyResult =
                SSL_get_verify_result(conn->get_socket().native_handle());

            if(verifyResult != 0 && m_verifyServerCertificate)
            {
                InvalidServerCertificate e{"OpenSSL error: " + std::to_string(verifyResult)};
                LOG(WARNING) << "Server certificate verification failed." <<
                                "OpenSSL error: " << e.what();
                throw e;
            }

            ConnectionError e{conn->get_ec().message()};
            LOG(WARNING) << "Connection failed: " << e.what();
            throw e;
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
    websocketpp::lib::error_code ec;
    const auto conn = m_endpoint.get_con_from_hdl(m_connection, ec);

    if(ec)
        LOG(WARNING) << "Error retrieving connection pointer: " << ec.message();
    else if(!conn)
        LOG(WARNING) << "Connection has no associated ASIO connection.";
    else
        LOG(WARNING) << "Connection interrupted: " << conn->get_ec().message();

    m_onErrorCallback();
}

} // namespace communication
} // namespace one
