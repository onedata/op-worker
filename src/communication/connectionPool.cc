/**
 * @file connectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/connectionPool.h"

#include "communication/connection.h"
#include "communication/exception.h"

#include <algorithm>
#include <cassert>
#include <exception>
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
                               std::string uri)
    : m_uri{std::move(uri)}
    , m_connectionsNumber{connectionsNumber}
{
    if(connectionsNumber == 0)
        throw std::invalid_argument{"Cannot create a ConnectionPool instance"
                                    " with 0 connections"};
}

ConnectionPool::~ConnectionPool()
{
    close();
}

void ConnectionPool::send(const std::string &payload)
{
    std::unique_lock<std::mutex> lock{m_connectionsMutex};
    addConnections();
    if(!m_connectionOpened.wait_for(lock, WAIT_FOR_CONNECTION,
                                    [&]{ return !m_openConnections.empty(); }))
        throw SendError{"No open connections available."};

    m_openConnections.front()->send(payload);

    m_openConnections.splice(m_openConnections.end(), m_openConnections,
                             m_openConnections.begin());
}

void ConnectionPool::setOnMessageCallback(std::function<void(const std::string&)> onMessageCallback)
{
    m_onMessageCallback = std::move(onMessageCallback);
}

void ConnectionPool::onFail(Connection &connection)
{
    namespace p = std::placeholders;
    std::lock_guard<std::mutex> guard{m_connectionsMutex};
    m_futureConnections.remove_if(std::bind(eq, p::_1, std::cref(connection)));
}

void ConnectionPool::onOpen(Connection &connection)
{
    namespace p = std::placeholders;
    std::lock_guard<std::mutex> guard{m_connectionsMutex};

    const auto it = std::find_if(m_futureConnections.begin(),
                                 m_futureConnections.end(),
                                 std::bind(eq, p::_1, std::cref(connection)));

    assert(it != m_futureConnections.cend());

    for(const auto &handshake: m_handshakes)
        (*it)->send(handshake());

    m_openConnections.splice(m_openConnections.begin(), m_futureConnections, it);
    m_connectionOpened.notify_all();
}

void ConnectionPool::onError(Connection &connection)
{
    namespace p = std::placeholders;
    std::lock_guard<std::mutex> guard{m_connectionsMutex};
    m_openConnections.remove_if(std::bind(eq, p::_1, std::cref(connection)));
}

void ConnectionPool::addHandshake(std::function<std::string()> handshake,
                                  std::function<std::string()> goodbye)
{
    std::lock_guard<std::mutex> guard{m_connectionsMutex};

    for(const auto &connection: m_openConnections)
        connection->send(handshake());

    m_handshakes.emplace_back(std::move(handshake));
    m_goodbyes.emplace_front(std::move(goodbye));
}

void ConnectionPool::close()
{
    std::unique_lock<std::mutex> lock{m_connectionsMutex};

    for(const auto &connection: m_openConnections)
        for(const auto &goodbye: m_goodbyes)
            connection->send(goodbye());

    m_futureConnections.clear();
    m_openConnections.clear();
}

void ConnectionPool::addHandshake(std::function<std::string ()> handshake)
{
    std::lock_guard<std::mutex> guard{m_connectionsMutex};

    for(const auto &connection: m_openConnections)
        connection->send(handshake());

    m_handshakes.emplace_back(std::move(handshake));
}

void ConnectionPool::addConnections()
{
    for(auto i = m_futureConnections.size() + m_openConnections.size();
        i < m_connectionsNumber; ++i)
    {
        m_futureConnections.emplace_back(createConnection());
    }
}

} // namespace communication
} // namespace veil
