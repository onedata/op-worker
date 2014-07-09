/**
 * @file connectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_CONNECTION_POOL_H
#define VEILHELPERS_CONNECTION_POOL_H


#include <memory>

namespace veil
{
namespace communication
{

class Connection;

class ConnectionPool
{
public:
    ConnectionPool(const unsigned int connectionsNumber);
    virtual ~ConnectionPool() = default;

    virtual std::shared_ptr<Connection> select() = 0;

protected:
    const unsigned int m_connectionsNumber;
};


} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CONNECTION_POOL_H
