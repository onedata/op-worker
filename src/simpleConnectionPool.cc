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

unsigned int maxConnectionCount = 10;

SimpleConnectionPool::SimpleConnectionPool(string hostname, int port, string certPath, bool (*updateCert)()) :
    m_hostname(hostname),
    updateCertCB(updateCert),
    m_port(port),
    m_certPath(certPath)
{   
}

SimpleConnectionPool::~SimpleConnectionPool() {}

boost::shared_ptr<CommunicationHandler> SimpleConnectionPool::selectConnection(bool forceNew, unsigned int nth) 
{
    boost::unique_lock< boost::mutex > lock(m_access);
    boost::shared_ptr<CommunicationHandler> conn;

    if(maxConnectionCount <= 0)
        maxConnectionCount = 1;

    list<pair<boost::shared_ptr<CommunicationHandler>, time_t> >::iterator it = m_connectionPool.begin();
    while(it != m_connectionPool.end()) {
        if(time(NULL) - (*it).second >= CONNECTION_MAX_ALIVE_TIME)
            it = m_connectionPool.erase(it);
        else ++it;
    }

    if(nth == 0 || --nth >= CommunicationHandler::getInstancesCount())
        forceNew = true;

    if(nth >= maxConnectionCount) 
        return boost::shared_ptr<CommunicationHandler>(); 

    int tryCount = 0; // After 1,5 sec allow to create additional connection
    while((m_connectionPool.empty() || forceNew) && CommunicationHandler::getInstancesCount() >= maxConnectionCount && tryCount++ < 30)
        m_accessCond.timed_wait(lock, posix_time::milliseconds(50));

    if(m_connectionPool.empty() || forceNew)
    {
        LOG(INFO) << "Theres no connections ready to be used. Creating new one";
        if(updateCertCB) {
            if(!updateCertCB()) 
            {
                LOG(ERROR) << "Could not find valid certificate.";
                return boost::shared_ptr<CommunicationHandler>();
            }
        }
        
        list<string> ips = dnsQuery(m_hostname);
        
        list<string>::iterator it = m_hostnamePool.begin();
        while(it != m_hostnamePool.end()) // Delete all hostname from m_hostnamePool which are not present in dnsQuery response
        {
            list<string>::const_iterator itIP = find(ips.begin(), ips.end(), (*it));
            if(itIP == ips.end())
                it = m_hostnamePool.erase(it);
            else ++it;
        } 
        
        for(it = ips.begin(); it != ips.end(); ++it) 
        {
            list<string>::const_iterator itIP = find(m_hostnamePool.begin(), m_hostnamePool.end(), (*it));
            if(itIP == m_hostnamePool.end())
                m_hostnamePool.push_back((*it));
        }
        
        int hostnameCount = m_hostnamePool.size();
        while(hostnameCount--) 
        {
            string connectTo = m_hostnamePool.front();
            m_hostnamePool.pop_front();
            m_hostnamePool.push_back(connectTo);
            
            conn.reset(new CommunicationHandler(connectTo, m_port, m_certPath)); 
            if(conn->openConnection() == 0) 
                break;
            
            conn.reset();
            LOG(WARNING) << "Cannot connect to host: " << connectTo << ":" << m_port;    
        }
        
        if(forceNew)    return conn; 
        else            m_connectionPool.push_back(pair<boost::shared_ptr<CommunicationHandler>, time_t>(conn, time(NULL)));
    }
    
    it = m_connectionPool.begin();
    nth = (nth >= m_connectionPool.size() ? m_connectionPool.size() - 1 : nth);
    advance(it, nth);
    conn = (*it).first;
    m_connectionPool.erase(it);
    return conn;
}


void SimpleConnectionPool::releaseConnection(boost::shared_ptr<CommunicationHandler> conn) 
{
    if(!conn)
        return;

    boost::unique_lock< boost::mutex > lock(m_access);
    m_connectionPool.push_front(pair<boost::shared_ptr<CommunicationHandler>, time_t>(conn, time(NULL)));
    m_accessCond.notify_one();
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
    }

    if(lst.size() == 0) {
        LOG(ERROR) << "DNS lookup failed for host: " << hostname;
        lst.push_back(hostname); // Make sure that returned list is not empty but adding argument to it
    }

    return lst;
}

} // namespace veil