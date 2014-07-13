/**
 * @file connectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/connectionPool.h"

#include "communication/connection.h"

#include <algorithm>
#include <cassert>
#include <future>

static constexpr std::chrono::seconds WAIT_FOR_CONNECTION{5}; // TODO: check this in current trunk

namespace
{
bool eq(const std::unique_ptr<veil::communication::Connection> &u,
        const veil::communication::Connection &r)
{
    return u.get() == &r;
}
}

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
    for(auto &connection: m_openConnections)
        connection->close();
}

void ConnectionPool::send(const std::string &payload)
{
    std::unique_lock<std::mutex> lock{m_connectionsMutex};
    for(unsigned int i = m_activeConnections; i < m_connectionsNumber; ++i)
        addConnection();

    if(!m_connectionOpened.wait_for(lock, WAIT_FOR_CONNECTION,
                                    [&]{ return !m_openConnections.empty(); }))
        return; // TODO: exception?

    m_openConnections.front()->send(payload);

    m_openConnections.splice(m_openConnections.end(), m_openConnections,
                             m_openConnections.begin());
}

void ConnectionPool::addConnection()
{
    m_futureConnections.emplace_back(createConnection());
    ++m_activeConnections;
}

void ConnectionPool::onFail(Connection &connection)
{
    namespace p = std::placeholders;
    std::lock_guard<std::mutex> guard{m_connectionsMutex};
    m_futureConnections.remove_if(std::bind(eq, p::_1, std::cref(connection)));
    --m_activeConnections;
}

void ConnectionPool::onOpen(Connection &connection)
{
    namespace p = std::placeholders;
    std::lock_guard<std::mutex> guard{m_connectionsMutex};

    const auto it = std::find_if(m_futureConnections.begin(),
                                 m_futureConnections.end(),
                                 std::bind(eq, p::_1, std::cref(connection)));

    assert(it != m_futureConnections.cend());

    m_openConnections.splice(m_openConnections.cbegin(), m_futureConnections, it);
    m_connectionOpened.notify_all();
}

void ConnectionPool::onError(Connection &connection)
{
    namespace p = std::placeholders;
    std::lock_guard<std::mutex> guard{m_connectionsMutex};
    m_openConnections.remove_if(std::bind(eq, p::_1, std::cref(connection)));
    --m_activeConnections;
}

} // namespace communication
} // namespace veil
