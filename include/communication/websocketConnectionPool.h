/**
 * @file websocketConnectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_WEBSOCKET_CONNECTION_POOL_H
#define VEILHELPERS_WEBSOCKET_CONNECTION_POOL_H


#include "connectionPool.h"

#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_client.hpp>

namespace veil
{
namespace communication
{

class WebsocketConnection;

class WebsocketConnectionPool: public ConnectionPool
{
    using endpoint_type = websocketpp::client<websocketpp::config::asio_tls_client>;

public:
    WebsocketConnectionPool(const unsigned int connectionsNumber);
    std::shared_ptr<Connection> select() override;

private:
    endpoint_type m_endpoint;
};


} // namespace communication
} // namespace veil


#endif // VEILHELPERS_WEBSOCKET_CONNECTION_POOL_H
