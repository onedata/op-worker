/**
 * @file connectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/connectionPool.h"

#include "communication/connection.h"

#include <future>

static constexpr std::chrono::seconds WAIT_FOR_CONNECTION{5}; // TODO: check this in current trunk

namespace veil
{
namespace communication
{

ConnectionPool::ConnectionPool(const unsigned int connectionsNumber,
                               std::shared_ptr<Mailbox> mailbox,
                               const std::string &uri)
    : m_connectionsNumber{connectionsNumber}
    , m_mailbox{std::move(mailbox)}
    , m_uri{uri}
{
    while(m_activeConnections < m_connectionsNumber)
        addConnection();
}

ConnectionPool::~ConnectionPool()
{
    for(auto connection: m_openConnections)
        connection->close();
}

std::function<void(const std::string&)> ConnectionPool::select()
{
    std::unique_lock<std::mutex> lock{m_connectionsMutex};
    for(unsigned int i = m_activeConnections; i < m_connectionsNumber; ++i)
        addConnection();

    if(!m_connectionOpened.wait_for(lock, WAIT_FOR_CONNECTION,
                                    [&]{ return !m_openConnections.empty(); }))
        return {}; // TODO: exception?

    const auto it = m_openConnections.begin();
    m_openConnections.splice(m_openConnections.end(), m_openConnections,
                             m_openConnections.begin());

    return std::bind(&Connection::send, *it);
}

void ConnectionPool::addConnection()
{
    m_futureConnections.emplace(createConnection());
    ++m_activeConnections;
}

void ConnectionPool::onFail(std::shared_ptr<Connection> connection)
{
    std::lock_guard<std::mutex> guard{m_connectionsMutex};
    m_futureConnections.erase(connection);
    --m_activeConnections;
}

void ConnectionPool::onOpen(std::shared_ptr<Connection> connection)
{
    std::lock_guard<std::mutex> guard{m_connectionsMutex};
    m_futureConnections.erase(connection);
    m_openConnections.emplace_front(connection);
    m_connectionOpened.notify_all();
}

void ConnectionPool::onError(std::shared_ptr<Connection> connection)
{
    std::lock_guard<std::mutex> guard{m_connectionsMutex};
    m_openConnections.remove(connection);
    --m_activeConnections;
}

} // namespace communication
} // namespace veil
