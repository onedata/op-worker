/**
 * @file websocketConnectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/websocketConnectionPool.h"

#include "communication/websocketConnection.h"

template class websocketpp::client<websocketpp::config::asio_tls_client>;

namespace veil
{
namespace communication
{

WebsocketConnectionPool::WebsocketConnectionPool(const unsigned int connectionsNumber,
                                                 std::shared_ptr<Mailbox> mailbox,
                                                 const std::string &uri)
    : ConnectionPool{connectionsNumber, std::move(mailbox), uri}
{
}

std::shared_ptr<Connection> WebsocketConnectionPool::select()
{
    // TODO: wait for connections
    const auto it = m_connections.begin();
    m_connections.splice(m_connections.end(), m_connections, m_connections.begin());
    return *it;
}

void WebsocketConnectionPool::addConnection() // TODO: async
{
    const auto connection = std::make_shared<WebsocketConnection>(m_mailbox,
                                                                  m_endpoint,
                                                                  m_uri);
    m_futureConnections.emplace(connection);
}

} // namespace communication
} // namespace veil
