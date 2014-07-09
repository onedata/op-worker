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
class Mailbox;

class ConnectionPool
{
public:
    ConnectionPool(const unsigned int connectionsNumber,
                   std::shared_ptr<Mailbox> mailbox,
                   const std::string &uri);

    virtual ~ConnectionPool() = default;

    virtual std::shared_ptr<Connection> select() = 0;

protected:
    const unsigned int m_connectionsNumber;
    const std::shared_ptr<Mailbox> m_mailbox;
    const std::string m_uri;
};


} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CONNECTION_POOL_H
