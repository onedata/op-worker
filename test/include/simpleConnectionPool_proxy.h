/**
 * @file simpleConnectionPool_proxy.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "simpleConnectionPool.h"

using namespace veil;

class ProxySimpleConnectionPool :
    public SimpleConnectionPool
{
public:
    ProxySimpleConnectionPool(std::string hostname, int port, cert_info_fun getCertInfo) :
        SimpleConnectionPool(hostname, port, getCertInfo)
    {
    }

    ~ProxySimpleConnectionPool() {}

    // Override
    boost::shared_ptr<CommunicationHandler> newConnection(SimpleConnectionPool::PoolType type)
    {
        boost::shared_ptr<CommunicationHandler> conn = boost::shared_ptr<CommunicationHandler>(new MockCommunicationHandler());
        m_connectionPools.at(type)->connections.push_front(make_pair(conn, time(NULL) + 20000));

        return conn;
    }

    void addConnection(SimpleConnectionPool::PoolType type, boost::shared_ptr<CommunicationHandler> conn)
    {
        m_connectionPools.at(type)->connections.push_back(pair<boost::shared_ptr<CommunicationHandler>, time_t>(conn, time(NULL) + 20000));
    }

    // Override
    std::list<std::string> dnsQuery(const std::string &hostname)
    {
        std::list<std::string> ret;
        ret.push_back(hostname);
        return ret;
    }
};
