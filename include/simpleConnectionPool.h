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
#include <ctime>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "communicationHandler.h"

#define DEFAULT_POOL_SIZE 2
#define MAX_CONNECTION_ERROR_COUNT 5

namespace veil {

typedef std::list<std::pair<boost::shared_ptr<CommunicationHandler>, time_t> > connection_pool_t;

class SimpleConnectionPool : public boost::enable_shared_from_this<SimpleConnectionPool>
{
public:
    
    enum PoolType {
        META_POOL = 0,  ///< Connection for meta data
        DATA_POOL       ///< Connection for file data
    };
    
    struct ConnectionPoolInfo {
        
        ConnectionPoolInfo(unsigned int s) : size(s) {}
        ConnectionPoolInfo() : size(DEFAULT_POOL_SIZE) {}
        
        connection_pool_t connections;
        int currWorkers;
        unsigned int size;
    };

    SimpleConnectionPool(std::string hostname, int port, std::string certPath, bool (*updateCert)(),int metaPoolSize = DEFAULT_POOL_SIZE,int dataPoolSize = DEFAULT_POOL_SIZE);
    virtual ~SimpleConnectionPool();
    
    virtual std::string getPeerCertificatePath();                           ///< Updates proxy certificate file if needed and returns its path.
    virtual void setPoolSize(PoolType type, unsigned int);                  ///< Sets size of connection pool. Default for each pool is: 2
    virtual void setPushCallback(std::string fuseId, push_callback);        ///< Sets fuseID and callback function that will be registered for
                                                                            ///< PUSH channel for every new META connection

    virtual void resetAllConnections(PoolType type);                        ///< Drops all connections from the pool.

    /**
     * Returns pointer to CommunicationHandler that is connected to cluster.
     * This method uses simple round-robin selection for all connections in pool.
     * It also creates new instances of CommunicationHandler if needed.
     */
    virtual boost::shared_ptr<CommunicationHandler> selectConnection(PoolType = META_POOL);
    virtual void releaseConnection(boost::shared_ptr<CommunicationHandler> conn);       ///< Returns CommunicationHandler's pointer ownership back to connection pool.
                                                                                        ///< @deprecated Since selectConnection does not pass connection ownership, this
                                                                                        ///< method is useless, so it does nothing.

protected:
    std::string          m_hostname;
    bool                 (*updateCertCB)();
    int                  m_port;
    std::string          m_certPath;
    std::string          m_fuseId;

    push_callback        m_pushCallback;
    
    boost::recursive_mutex      m_access;
    boost::condition_variable   m_accessCond;
    std::map<PoolType, ConnectionPoolInfo>  m_connectionPools;                      ///< Connection pool. @see SimpleConnectionPool::selectConnection
    std::list<std::string> m_hostnamePool;
    
    virtual boost::shared_ptr<CommunicationHandler> newConnection(PoolType type);   ///< Creates new active connection and adds it to connection pool. Convenience method for testing (makes connection mocking easier) 
    virtual std::list<std::string> dnsQuery(std::string hostname);                  ///< Fetch IP list from DNS for given hostname.

    /// Increments givent int while constructing and deincrement while destructing. 
    struct CounterRAII {
        int &c;

        CounterRAII(int &c) 
          : c(c) 
        {
            ++c;
        }

        ~CounterRAII() 
        {
            --c;
        }

    };
};

} // namespace veil

#endif // SIMPLE_CONNECTION_POOL_H

