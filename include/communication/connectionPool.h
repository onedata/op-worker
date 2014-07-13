/**
 * @file connectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_CONNECTION_POOL_H
#define VEILHELPERS_CONNECTION_POOL_H


#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

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

    virtual ~ConnectionPool();
    ConnectionPool(ConnectionPool&&) = default;
    ConnectionPool &operator=(ConnectionPool&&) & = default;
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool &operator=(const ConnectionPool&) = delete;

    void send(const std::string &payload);


protected:
    void addConnection();
    void onFail(Connection &connection);
    void onOpen(Connection &connection);
    void onError(Connection &connection);

    virtual std::unique_ptr<Connection> createConnection() = 0;

    const unsigned int m_connectionsNumber;
    const std::shared_ptr<Mailbox> m_mailbox;
    const std::string m_uri;
    std::mutex m_connectionsMutex;
    std::condition_variable m_connectionOpened;
    std::list<std::unique_ptr<Connection>> m_futureConnections;
    std::list<std::unique_ptr<Connection>> m_openConnections;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CONNECTION_POOL_H
