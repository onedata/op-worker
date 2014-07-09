/**
 * @file connectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/connectionPool.h"

namespace veil
{
namespace communication
{

ConnectionPool::ConnectionPool(const unsigned int connectionsNumber)
    : m_connectionsNumber(connectionsNumber)
{
}

} // namespace communication
} // namespace veil
