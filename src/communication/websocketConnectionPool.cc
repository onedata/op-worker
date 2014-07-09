/**
 * @file websocketConnectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/websocketConnectionPool.h"

namespace veil
{
namespace communication
{

WebsocketConnectionPool::WebsocketConnectionPool(const unsigned int connectionsNumber)
    : ConnectionPool{connectionsNumber}
{

}

std::shared_ptr<Connection> WebsocketConnectionPool::select()
{

}

} // namespace communication
} // namespace veil
