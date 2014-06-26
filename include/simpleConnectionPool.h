/**
 * @file simpleConnectionPool.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */


#ifndef SIMPLE_CONNECTION_POOL_H
#define SIMPLE_CONNECTION_POOL_H

#include "communicationHandler.h"
#include "veilErrors.h"

#include <atomic>
#include <ctime>
#include <list>
#include <memory>
#include <mutex>
#include <thread>

#define DEFAULT_POOL_SIZE 2
#define MAX_CONNECTION_ERROR_COUNT 5

namespace veil {

using connection_pool_t = std::list<std::pair<std::shared_ptr<CommunicationHandler>, time_t>>;

class SimpleConnectionPool : public std::enable_shared_from_this<SimpleConnectionPool>
{
public:

    enum PoolType {
        META_POOL = 0,  ///< Connection for meta data
        DATA_POOL       ///< Connection for file data
    };

    struct ConnectionPoolInfo {

        ConnectionPoolInfo(unsigned int s) : currWorkers(0), size(s) {}
        ConnectionPoolInfo() : size(DEFAULT_POOL_SIZE) {}

        connection_pool_t connections;
        int currWorkers;
        unsigned int size;
    };

    SimpleConnectionPool(const std::string &hostname, int port, cert_info_fun, const bool checkCertificate = false, int metaPoolSize = DEFAULT_POOL_SIZE, int dataPoolSize = DEFAULT_POOL_SIZE);
    virtual ~SimpleConnectionPool();

    virtual void setPoolSize(PoolType type, unsigned int);                  ///< Sets size of connection pool. Default for each pool is: 2
    virtual void setPushCallback(const std::string &fuseId, push_callback); ///< Sets fuseID and callback function that will be registered for
                                                                            ///< PUSH channel for every new META connection

    virtual void resetAllConnections(PoolType type);                        ///< Drops all connections from the pool.

    /**
     * Returns pointer to CommunicationHandler that is connected to cluster.
     * This method uses simple round-robin selection for all connections in pool.
     * It also creates new instances of CommunicationHandler if needed.
     */
    virtual std::shared_ptr<CommunicationHandler> selectConnection(PoolType = META_POOL);
    virtual void releaseConnection(std::shared_ptr<CommunicationHandler> conn);       ///< Returns CommunicationHandler's pointer ownership back to connection pool.
                                                                                      ///< @deprecated Since selectConnection does not pass connection ownership, this
                                                                                      ///< method is useless, so it does nothing.

    /**
     * @returns Last error encountered while creating or managing connections.
     */
    error::Error getLastError() const;

protected:
    std::string          m_hostname;
    int                  m_port;
    cert_info_fun        m_getCertInfo;
    std::string          m_fuseId;

    push_callback        m_pushCallback;

    std::recursive_mutex      m_access;
    std::condition_variable   m_accessCond;
    std::map<PoolType, ConnectionPoolInfo>  m_connectionPools;                      ///< Connection pool. @see SimpleConnectionPool::selectConnection
    std::list<std::string> m_hostnamePool;

    virtual std::shared_ptr<CommunicationHandler> newConnection(PoolType type);     ///< Creates new active connection and adds it to connection pool. Convenience method for testing (makes connection mocking easier)
    virtual std::list<std::string> dnsQuery(const std::string &hostname);           ///< Fetch IP list from DNS for given hostname.

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

private:
    std::atomic<error::Error> m_lastError;
    const bool m_checkCertificate;
};

} // namespace veil

#endif // SIMPLE_CONNECTION_POOL_H

