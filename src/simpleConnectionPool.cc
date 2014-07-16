/**
 * @file simpleConnectionPool.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "simpleConnectionPool.h"
#include "communicationHandler.h"
#include "logging.h"
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <iterator>
#include <algorithm>
#include <boost/thread/thread_time.hpp>
#include <exception>
#include "helpers/storageHelperFactory.h"

using namespace boost;
using namespace std;

namespace veil {

SimpleConnectionPool::SimpleConnectionPool(const string &hostname, int port,
                                           cert_info_fun certInfoFun,
                                           const bool checkCertificate,
                                           int metaPoolSize, int dataPoolSize)
    : m_hostname(hostname)
    , m_port(port)
    , m_getCertInfo(certInfoFun)
    , m_checkCertificate{checkCertificate}
{
    m_connectionPools.emplace(META_POOL, std::unique_ptr<ConnectionPoolInfo>(new ConnectionPoolInfo(certInfoFun, metaPoolSize)));
    m_connectionPools.emplace(DATA_POOL, std::unique_ptr<ConnectionPoolInfo>(new ConnectionPoolInfo(certInfoFun, dataPoolSize)));
}

void SimpleConnectionPool::resetAllConnections(PoolType type)
{
    m_connectionPools.at(type)->connections.clear();
    setPoolSize(type, m_connectionPools.at(type)->size); // Force connection reinitialization
}

boost::shared_ptr<CommunicationHandler> SimpleConnectionPool::newConnection(PoolType type)
{
    boost::unique_lock< boost::recursive_mutex > lock(m_access);

    ConnectionPoolInfo &poolInfo = *m_connectionPools.at(type);
    boost::shared_ptr<CommunicationHandler> conn;

    CounterRAII sc(poolInfo.currWorkers);

    lock.unlock();
    list<string> ips = dnsQuery(m_hostname);
    lock.lock();

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

    // Connect to first working host
    int hostnameCount = m_hostnamePool.size();
    while(hostnameCount--)
    {
        string connectTo = m_hostnamePool.front();
        m_hostnamePool.pop_front();
        m_hostnamePool.push_back(connectTo);

        lock.unlock();

        conn.reset(new CommunicationHandler(connectTo, m_port, m_getCertInfo, m_checkCertificate, poolInfo.endpoint));
        conn->setFuseID(m_fuseId);  // Set FuseID that shall be used by this connection as session ID
        if(m_pushCallback)                          // Set callback that shall be used for PUSH messages and error messages
            conn->setPushCallback(m_pushCallback);  // Note that this doesnt enable/register PUSH channel !

        if(m_pushCallback && m_fuseId.size() > 0 && type == META_POOL) // Enable PUSH channel (will register itself when possible)
            conn->enablePushChannel();

        if(conn->openConnection() == 0) {
            break;
        }

        m_lastError.store(conn->getLastError());

        lock.lock();
        conn.reset();
        LOG(WARNING) << "Cannot connect to host: " << connectTo << ":" << m_port;

    }

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

    ConnectionPoolInfo &poolInfo = *m_connectionPools.at(type);

    // Delete first connection in pool if its error counter is way to big
    if(poolInfo.connections.size() > 0 && poolInfo.connections.front().first->getErrorCount() > MAX_CONNECTION_ERROR_COUNT) {
        LOG(INFO) << "Connection " << poolInfo.connections.front().first << " emited to many errors. Reinitializing object.";
        poolInfo.connections.pop_front();
    }

    // Remove redundant connections
    while(poolInfo.connections.size() > poolInfo.size)
        poolInfo.connections.pop_back();

    // Check if pool size matches config
    long toStart = poolInfo.size - poolInfo.connections.size() - poolInfo.currWorkers;
    if(poolInfo.connections.size() == 0 && toStart <= 0)
        toStart = 1;

    DLOG(INFO) << "Current pool size: " << poolInfo.connections.size() << ", connections in construction: " << poolInfo.currWorkers << ", expected: " << poolInfo.size;

    while(toStart --> 0) // Current pool is too small, we should create some connection(s)
    {
        LOG(INFO) << "Connection pool (" << type << " is to small (" << poolInfo.connections.size() << " connections - expected: " << poolInfo.size << "). Opening new connection...";

        if(poolInfo.connections.size() > 0) {
            boost::thread t = boost::thread(boost::bind(&SimpleConnectionPool::newConnection, shared_from_this(), type));
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

error::Error SimpleConnectionPool::getLastError() const
{
    return m_lastError.load();
}

void SimpleConnectionPool::setPoolSize(PoolType type, unsigned int s)
{
    boost::unique_lock< boost::recursive_mutex > lock(m_access);
    m_connectionPools.at(type)->size = s;

    // Insert new connections to pool if needed (async)
    long toStart = m_connectionPools.at(type)->size - m_connectionPools.at(type)->connections.size();
    while(toStart-- > 0) {
        boost::thread t = boost::thread(boost::bind(&SimpleConnectionPool::newConnection, shared_from_this(), type));
        t.detach();
    }
}

void SimpleConnectionPool::setPushCallback(const string &fuseId, push_callback hdl)
{
    m_fuseId = fuseId;
    m_pushCallback = hdl;
}

list<string> SimpleConnectionPool::dnsQuery(const string &hostname)
{
    boost::unique_lock< boost::recursive_mutex > lock(m_access);

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

SimpleConnectionPool::ConnectionPoolInfo::ConnectionPoolInfo(cert_info_fun getCertInfo, unsigned int s)
    : size(s)
    , m_getCertInfo{std::move(getCertInfo)}
{
    LOG(INFO) << "Initializing a WebSocket endpoint";
    websocketpp::lib::error_code ec;
    endpoint->clear_access_channels(websocketpp::log::alevel::all);
    endpoint->clear_error_channels(websocketpp::log::elevel::all);
    endpoint->init_asio(ec);
    endpoint->start_perpetual();

    if(ec)
    {
        LOG(ERROR) << "Cannot initlize WebSocket endpoint; terminating.";
        std::terminate();
    }

    // Start worker thread
    m_ioWorker = boost::thread(&ws_client::run, endpoint);

    endpoint->set_tls_init_handler(bind(&ConnectionPoolInfo::onTLSInit, this, ::_1));
    endpoint->set_socket_init_handler(bind(&ConnectionPoolInfo::onSocketInit, this, ::_1, ::_2));
}

SimpleConnectionPool::ConnectionPoolInfo::~ConnectionPoolInfo()
{
    LOG(INFO) << "Stopping the WebSocket endpoint";
    endpoint->stop_perpetual();

    for(auto &con: connections)
        con.first->closeConnection();

    LOG(INFO) << "Stopping WebSocket endpoint worker thread";
    m_ioWorker.join();
}

context_ptr SimpleConnectionPool::ConnectionPoolInfo::onTLSInit(websocketpp::connection_hdl hdl)
{
    if (!m_getCertInfo) {
        LOG(ERROR) << "Cannot get CertificateInfo due to null getter";
        return context_ptr();
    }

    CertificateInfo certInfo = m_getCertInfo();

    try {
        context_ptr ctx(new boost::asio::ssl::context(boost::asio::ssl::context::sslv3));

        ctx->set_options(boost::asio::ssl::context::default_workarounds |
                         boost::asio::ssl::context::no_sslv2 |
                         boost::asio::ssl::context::single_dh_use);

        ctx->set_default_verify_paths();
        ctx->set_verify_mode(helpers::config::checkCertificate.load()
                             ? boost::asio::ssl::verify_peer
                             : boost::asio::ssl::verify_none);

        boost::asio::ssl::context_base::file_format file_format; // Certificate format
        if(certInfo.cert_type == CertificateInfo::ASN1) {
            file_format = boost::asio::ssl::context::asn1;
        } else {
            file_format = boost::asio::ssl::context::pem;
        }

        if(certInfo.cert_type == CertificateInfo::P12) {
            LOG(ERROR) << "Unsupported certificate format: P12";
            return context_ptr();
        }

        if(boost::asio::buffer_size(certInfo.chain_data) && boost::asio::buffer_size(certInfo.chain_data)) {
            ctx->use_certificate_chain(certInfo.chain_data);
            ctx->use_private_key(certInfo.key_data, file_format);
        } else {
            ctx->use_certificate_chain_file(certInfo.user_cert_path);
            ctx->use_private_key_file(certInfo.user_key_path, file_format);
        }

        return ctx;

    } catch (boost::system::system_error& e) {
        LOG(ERROR) << "Cannot initialize TLS socket due to: " << e.what()
                   << " with cert file: " << certInfo.user_cert_path << " and key file: "
                   << certInfo.user_key_path;
    }

    return context_ptr();
}

void SimpleConnectionPool::ConnectionPoolInfo::onSocketInit(websocketpp::connection_hdl hdl, socket_type &socket)
{
    // Disable socket delay
    socket.lowest_layer().set_option(boost::asio::ip::tcp::no_delay(true));
}

} // namespace veil
