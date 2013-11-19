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
    ProxySimpleConnectionPool(std::string hostname, int port, std::string cert, bool(*fun)()) : 
        SimpleConnectionPool(hostname, port, cert, fun)
    {
    }

    ~ProxySimpleConnectionPool() {}

    void addConnection(boost::shared_ptr<CommunicationHandler> conn)
    {
        m_connectionPool.push_back(pair<boost::shared_ptr<CommunicationHandler>, time_t>(conn, time(NULL) + 20000));
    }

    std::list<std::string> dnsQuery(std::string hostname) 
    {
        std::list<std::string> ret;
        ret.push_back(hostname);
        return ret;
    }
};