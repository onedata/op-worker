/**
 * @file websocketConnectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/websocketConnectionPool.h"

#include "communication/websocketConnection.h"
#include "make_unique.h"

#include <chrono>
#include <functional>
#include <future>

template class websocketpp::client<websocketpp::config::asio_tls_client>;

namespace veil
{
namespace communication
{

class CertificateData;

WebsocketConnectionPool::WebsocketConnectionPool(const unsigned int connectionsNumber,
                                                 std::shared_ptr<Mailbox> mailbox,
                                                 const std::string &uri,
                                                 std::shared_ptr<CertificateData> certificateData,
                                                 const bool verifyServerCertificate)
    : ConnectionPool{connectionsNumber, std::move(mailbox), uri}
    , m_certificateData{std::move(certificateData)}
    , m_verifyServerCertificate{verifyServerCertificate}
{
#ifndef NDEBUG
    m_endpoint.clear_access_channels(websocketpp::log::alevel::all);
    m_endpoint.clear_error_channels(websocketpp::log::elevel::all);
#endif

    m_endpoint.init_asio();
    m_endpoint.start_perpetual();

    m_ioThread = std::thread{&endpoint_type::run, &m_endpoint};
}

WebsocketConnectionPool::~WebsocketConnectionPool()
{
    m_endpoint.stop_perpetual();
    m_ioThread.join();
}

std::unique_ptr<Connection> WebsocketConnectionPool::createConnection()
{
    return std::make_unique<WebsocketConnection>(
                m_mailbox,
                std::bind(&WebsocketConnectionPool::onFail, this, std::placeholders::_1),
                std::bind(&WebsocketConnectionPool::onOpen, this, std::placeholders::_1),
                std::bind(&WebsocketConnectionPool::onError, this, std::placeholders::_1),
                m_endpoint, m_uri, m_certificateData, m_verifyServerCertificate);
}

} // namespace communication
} // namespace veil
