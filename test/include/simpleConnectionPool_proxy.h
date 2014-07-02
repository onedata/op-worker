/**
 * @file simpleConnectionPool_proxy.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef SIMPLE_CONNECTION_POOL_PROXY_H
#define SIMPLE_CONNECTION_POOL_PROXY_H


#include "simpleConnectionPool.h"

#include "communicationHandler_mock.h"

class ProxySimpleConnectionPool: public veil::SimpleConnectionPool
{
public:
    ProxySimpleConnectionPool(std::string hostname, int port, veil::cert_info_fun getCertInfo) :
        SimpleConnectionPool{hostname, port, getCertInfo}
    {
    }

    std::shared_ptr<veil::CommunicationHandler> newConnection(SimpleConnectionPool::PoolType type) override
    {
        auto conn = std::make_shared<MockCommunicationHandler>();
        m_connectionPools[type].connections.emplace_front(conn, time(NULL) + 20000);

        return conn;
    }

    void addConnection(SimpleConnectionPool::PoolType type, std::shared_ptr<veil::CommunicationHandler> conn)
    {
        m_connectionPools[type].connections.emplace_back(conn, time(NULL) + 20000);
    }

    std::list<std::string> dnsQuery(const std::string &hostname) override
    {
        std::list<std::string> ret;
        ret.push_back(hostname);
        return ret;
    }
};


#endif // SIMPLE_CONNECTION_POOL_PROXY_H
