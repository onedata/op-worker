/**
 * @file simpleConnectionPool.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "simpleConnectionPool.h"
#include "glog/logging.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <iterator>
#include <algorithm>

using namespace boost;
using namespace std;

namespace veil {

SimpleConnectionPool::SimpleConnectionPool(string hostname, int port, string certPath, bool (*updateCert)()) :
    m_hostname(hostname),
    updateCertCB(updateCert),
    m_port(port),
    m_certPath(certPath)
{   
}

SimpleConnectionPool::~SimpleConnectionPool() {}

shared_ptr<CommunicationHandler> SimpleConnectionPool::selectConnection(bool forceNew, unsigned int nth) 
{
    boost::unique_lock< boost::shared_mutex > lock(m_access);
    shared_ptr<CommunicationHandler> conn;

    list<pair<shared_ptr<CommunicationHandler>, time_t> >::iterator it = m_connectionPool.begin();
    while(it != m_connectionPool.end()) {
        if(time(NULL) - (*it).second >= CONNECTION_MAX_ALIVE_TIME)
            it = m_connectionPool.erase(it);
        else ++it;
    }

    if(nth == 0 || --nth >= m_connectionPool.size()) {
        forceNew = true;
        nth = m_connectionPool.size() - 1; // Just to be sure
    }

    if(m_connectionPool.empty() || forceNew)
    {
        LOG(INFO) << "Theres no connections ready to be used. Creating new one";
        if(updateCertCB) {
            if(!updateCertCB()) 
            {
                LOG(ERROR) << "Could not find valid certificate.";
                return shared_ptr<CommunicationHandler>();
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
        else            m_connectionPool.push_back(pair<shared_ptr<CommunicationHandler>, time_t>(conn, time(NULL)));
    }
    
    it = m_connectionPool.begin();
    advance(it, nth);
    conn = (*it).first;
    m_connectionPool.erase(it);
    return conn;
}


void SimpleConnectionPool::releaseConnection(shared_ptr<CommunicationHandler> conn) 
{
    boost::unique_lock< boost::shared_mutex > lock(m_access);
    m_connectionPool.push_front(pair<shared_ptr<CommunicationHandler>, time_t>(conn, time(NULL)));
}

list<string> SimpleConnectionPool::dnsQuery(string hostname) 
{
    list<string> lst;
    struct addrinfo *result;
    struct addrinfo *res;
    if(getaddrinfo(hostname.c_str(), NULL, NULL, &result) == 0) {
        for(res = result; res != NULL; res = res->ai_next) {
            char ip[NI_MAXHOST] = "";
            if(getnameinfo(res->ai_addr, res->ai_addrlen, ip, NI_MAXHOST, NULL, 0, 0) == 0)
                lst.push_back(string(ip));
        }       
    }

    if(lst.size() == 0) {
        LOG(ERROR) << "DNS lookup failed for host: " << hostname;
        lst.push_back(hostname); // Make sure that returned list is not empty but adding argument to it
    }

    return lst;
}

} // namespace veil