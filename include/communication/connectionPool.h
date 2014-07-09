/**
 * @file connectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_CONNECTION_POOL_H
#define VEILHELPERS_CONNECTION_POOL_H


#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_set>

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

    std::function<void(const std::string&)> select();

    void addConnection();
    void onFail(std::shared_ptr<Connection> connection);
    void onOpen(std::shared_ptr<Connection> connection);
    void onError(std::shared_ptr<Connection> connection);

protected:
    virtual std::shared_ptr<Connection> createConnection() = 0;

    const unsigned int m_connectionsNumber;
    const std::shared_ptr<Mailbox> m_mailbox;
    const std::string m_uri;
    std::mutex m_connectionsMutex;
    std::condition_variable m_connectionOpened;
    std::atomic<unsigned int> m_activeConnections{0};
    std::unordered_set<std::shared_ptr<Connection>> m_futureConnections;
    std::list<std::shared_ptr<Connection>> m_openConnections;
};


} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CONNECTION_POOL_H
