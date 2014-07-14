/**
 * @file connectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_CONNECTION_POOL_H
#define VEILHELPERS_CONNECTION_POOL_H


#include <condition_variable>
#include <functional>
#include <list>
#include <memory>
#include <mutex>

namespace veil
{
namespace communication
{

class Connection;

class ConnectionPool
{
public:
    ConnectionPool(const unsigned int connectionsNumber,
                   std::string uri);

    virtual ~ConnectionPool() = default;
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool &operator=(const ConnectionPool&) = delete;

    void send(const std::string &payload);
    void setOnMessageCallback(std::function<void(const std::string&)> onMessageCallback);

protected:
    void addConnection();
    void onFail(Connection &connection);
    void onOpen(Connection &connection);
    void onError(Connection &connection);

    virtual std::unique_ptr<Connection> createConnection() = 0;

    const unsigned int m_connectionsNumber;
    const std::string m_uri;
    std::function<void(const std::string&)> m_onMessageCallback = [](const std::string&){};
    std::mutex m_connectionsMutex;
    std::condition_variable m_connectionOpened;
    std::list<std::unique_ptr<Connection>> m_futureConnections;
    std::list<std::unique_ptr<Connection>> m_openConnections;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CONNECTION_POOL_H
