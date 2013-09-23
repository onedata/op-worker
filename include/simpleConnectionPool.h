/**
 * @file simpleConnectionPool.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */


#ifndef SIMPLE_CONNECTION_POOL_H
#define SIMPLE_CONNECTION_POOL_H

#include <boost/thread.hpp>
#include <list>
#include <time.h>
#include <boost/shared_ptr.hpp>
#include "communicationHandler.h"

#define CONNECTION_MAX_ALIVE_TIME 5 // in seconds

namespace veil {

class SimpleConnectionPool
{
public:

    SimpleConnectionPool(std::string hostname, int port, std::string certPath, bool (*updateCert)());
    virtual ~SimpleConnectionPool();

    /**
     * Returns pointer to CommunicationHandler that is not used at this moment.
     * This method uses simple round-robin selection from FslogicProxy::m_connectionPool.
     * It also creates new instances of CommunicationHandler if needed.
     * @warning You are reciving CommunicationHandler ownership ! It means that you have to either destroy
     * or return ownership (prefered way, since this connection could be reused) via FslogicProxy::releaseConnection.
     * @see FslogicProxy::releaseConnection
     */             
    virtual boost::shared_ptr<CommunicationHandler> selectConnection(bool forceNew = false, unsigned int nth = 1);
    virtual void releaseConnection(boost::shared_ptr<CommunicationHandler> conn);       ///< Returns CommunicationHandler's pointer ownership back to connection pool. Returned connection can be selected later again
                                                                                        ///< and be reused to boost preformance.

protected:
    std::string          m_hostname;
    bool            (*updateCertCB)();
    int             m_port;
    std::string          m_certPath;

    boost::shared_mutex m_access;
    std::list<std::pair<boost::shared_ptr<CommunicationHandler>, time_t> > m_connectionPool;   ///< Connection pool. @see SimpleConnectionPool::selectConnection
    std::list<std::string> m_hostnamePool;
    
    virtual std::list<std::string> dnsQuery(std::string hostname);          ///< Fetch IP list from DNS for given hostname.
};

} // namespace veil

#endif // SIMPLE_CONNECTION_POOL_H

