/**
 * @file simpleConnectionPool.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "simpleConnectionPool.h"
#include "glog/logging.h"
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <iterator>
#include <algorithm>
#include <boost/thread/thread_time.hpp>

using namespace boost;
using namespace std;

namespace veil {

SimpleConnectionPool::SimpleConnectionPool(string hostname, int port, string certPath, bool (*updateCert)(),int metaPoolSize,int dataPoolSize) :
    m_hostname(hostname),
    updateCertCB(updateCert),
    m_port(port),
    m_certPath(certPath)
{
    m_connectionPools[META_POOL] = ConnectionPoolInfo(metaPoolSize);
    m_connectionPools[DATA_POOL] = ConnectionPoolInfo(dataPoolSize);
}

SimpleConnectionPool::~SimpleConnectionPool() {}

string SimpleConnectionPool::getPeerCertificatePath() 
{
    //                 Disable certificate update for now (due to globus memory leak)
    if(updateCertCB && false) 
    {
        if(!updateCertCB())
        {
            LOG(ERROR) << "Could not find valid certificate.";
        }
    }

    return m_certPath;
}

void SimpleConnectionPool::resetAllConnections(PoolType type)
{
    m_connectionPools[type].connections.clear();
    setPoolSize(type, m_connectionPools[type].size); // Force connection reinitialization
}

boost::shared_ptr<CommunicationHandler> SimpleConnectionPool::newConnection(PoolType type)
{
	cout << "nc1"<<endl;
    boost::unique_lock< boost::recursive_mutex > lock(m_access);
    cout << "nc2"<<endl;

    ConnectionPoolInfo &poolInfo = m_connectionPools[type];
    boost::shared_ptr<CommunicationHandler> conn;

	cout << "nc3"<<endl;

    // Check if certificate is OK and generate new one if needed and possible
    //                 Disable certificate update for now (due to globus memory leak)
    if(updateCertCB && false) {
        if(!updateCertCB())
        {
            LOG(ERROR) << "Could not find valid certificate.";
            return boost::shared_ptr<CommunicationHandler>();
        }
    }
	cout << "nc4"<<endl;

    lock.unlock();
    list<string> ips = dnsQuery(m_hostname);
    lock.lock();
	cout << "nc5"<<endl;

    list<string>::iterator it = m_hostnamePool.begin();
    while(it != m_hostnamePool.end()) // Delete all hostname from m_hostnamePool which are not present in dnsQuery response
    {
        list<string>::const_iterator itIP = find(ips.begin(), ips.end(), (*it));
        if(itIP == ips.end())
            it = m_hostnamePool.erase(it);
        else ++it;
    }
    cout << "nc6"<<endl;
    for(it = ips.begin(); it != ips.end(); ++it)
    {
        list<string>::const_iterator itIP = find(m_hostnamePool.begin(), m_hostnamePool.end(), (*it));
        if(itIP == m_hostnamePool.end())
            m_hostnamePool.push_back((*it));
    }
    cout << "nc7"<<endl;
    // Connect to first working host
    int hostnameCount = m_hostnamePool.size();
    while(hostnameCount--)
    {
        string connectTo = m_hostnamePool.front();
        m_hostnamePool.pop_front();
        m_hostnamePool.push_back(connectTo);
        
        lock.unlock();

        conn.reset(new CommunicationHandler(connectTo, m_port, getPeerCertificatePath()));
        conn->setCertFun(boost::bind(&SimpleConnectionPool::getPeerCertificatePath, this));
        conn->setFuseID(m_fuseId);  // Set FuseID that shall be used by this connection as session ID
        if(m_pushCallback)                          // Set callback that shall be used for PUSH messages and error messages
            conn->setPushCallback(m_pushCallback);  // Note that this doesnt enable/register PUSH channel !

        if(m_pushCallback && m_fuseId.size() > 0 && type == META_POOL) // Enable PUSH channel (will register itself when possible)
            conn->enablePushChannel();

        if(conn->openConnection() == 0) {
            break;
        }

        lock.lock();
        conn.reset();
        LOG(WARNING) << "Cannot connect to host: " << connectTo << ":" << m_port;
        
    }
    cout << "nc8"<<endl;
    if(conn)
        poolInfo.connections.push_front(make_pair(conn, time(NULL)));
    else
        LOG(ERROR) << "Opening new connection (type: " << type << ") failed!";
    
    return conn;
}
    
boost::shared_ptr<CommunicationHandler> SimpleConnectionPool::selectConnection(PoolType type)
{
    boost::unique_lock< boost::recursive_mutex > lock(m_access);
    boost::shared_ptr<CommunicationHandler> conn;
    
    ConnectionPoolInfo &poolInfo = m_connectionPools[type];
    
    // Delete first connection in pool if its error counter is way to big
    if(poolInfo.connections.size() > 0 && poolInfo.connections.front().first->getErrorCount() > MAX_CONNECTION_ERROR_COUNT)
        poolInfo.connections.pop_front();
    
    // Remove redundant connections
    while(poolInfo.connections.size() > poolInfo.size)
        poolInfo.connections.pop_back();
    
    // Check if pool size matches config
    long toStart = poolInfo.size - poolInfo.connections.size();
    while(toStart-- > 0) // Current pool is too small, we should create some connection(s)
    {
        LOG(INFO) << "Connection pool (" << type << " is to small (" << poolInfo.connections.size() << " connections - expected: " << poolInfo.size << "). Opening new connection...";
        
        if(poolInfo.connections.size() > 0) {
            boost::thread t = thread(boost::bind(&SimpleConnectionPool::newConnection, shared_from_this(), type));
            t.detach();
        }
        else
            conn = newConnection(type);
    }
    
    if(poolInfo.connections.size() > 0)
    {
        conn = poolInfo.connections.front().first;
        
        // Round-robin
        poolInfo.connections.push_back(poolInfo.connections.front());
        poolInfo.connections.pop_front();
    }
    
    return conn;
}

void SimpleConnectionPool::releaseConnection(boost::shared_ptr<CommunicationHandler> conn) 
{
    return;
}
    
void SimpleConnectionPool::setPoolSize(PoolType type, unsigned int s)
{
    boost::unique_lock< boost::recursive_mutex > lock(m_access);
    m_connectionPools[type].size = s;

    // Insert new connections to pool if needed (async)
    long toStart = m_connectionPools[type].size - m_connectionPools[type].connections.size();
    while(toStart-- > 0) {
        boost::thread t = thread(boost::bind(&SimpleConnectionPool::newConnection, shared_from_this(), type));
        t.detach();
    }
}
    
void SimpleConnectionPool::setPushCallback(std::string fuseId, push_callback hdl)
{
    m_fuseId = fuseId;
    m_pushCallback = hdl;
}

list<string> SimpleConnectionPool::dnsQuery(string hostname)
{
    list<string> lst;
    struct addrinfo *result;
    struct addrinfo *res;

    if(getaddrinfo(hostname.c_str(), NULL, NULL, &result) == 0) {
        for(res = result; res != NULL; res = res->ai_next) {
            char ip[INET_ADDRSTRLEN + 1] = "";
            switch(res->ai_addr->sa_family) {
                case AF_INET:
                    if(inet_ntop(res->ai_addr->sa_family, &((struct sockaddr_in*)res->ai_addr)->sin_addr, ip, INET_ADDRSTRLEN + 1) != NULL)
                        lst.push_back(string(ip));  
                case AF_INET6:
                    // Not supported
                    break;
                default:
                    break;
            }
        }
        freeaddrinfo(result);
    }

    if(lst.size() == 0) {
        LOG(ERROR) << "DNS lookup failed for host: " << hostname;
        lst.push_back(hostname); // Make sure that returned list is not empty but adding argument to it
    }

    return lst;
}

} // namespace veil
