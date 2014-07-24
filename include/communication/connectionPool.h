/**
 * @file connectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATION_CONNECTION_POOL_H
#define VEILHELPERS_COMMUNICATION_CONNECTION_POOL_H


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

    virtual ~ConnectionPool();

    virtual void send(const std::string &payload);
    virtual void setOnMessageCallback(std::function<void(const std::string&)> onMessageCallback);
    virtual std::function<void()> addHandshake(std::function<std::string()> handshake);
    virtual std::function<void()> addHandshake(std::function<std::string()> handshake,
                                               std::function<std::string()> goodbye);

protected:
    void close();
    void addConnections();
    void onFail(Connection &connection);
    void onOpen(Connection &connection);
    void onError(Connection &connection);

    virtual std::unique_ptr<Connection> createConnection() = 0;

    std::function<void(const std::string&)> m_onMessageCallback = [](const std::string&){};
    const std::string m_uri;

private:
    void sendHandshakeMessage(Connection &conn, const std::string &payload);

    const unsigned int m_connectionsNumber;
    std::mutex m_connectionsMutex;
    std::condition_variable m_connectionOpened;
    std::list<std::unique_ptr<Connection>> m_futureConnections;
    std::list<std::unique_ptr<Connection>> m_openConnections;
    std::list<std::function<std::string()>> m_handshakes;
    std::list<std::function<std::string()>> m_goodbyes;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATION_CONNECTION_POOL_H
