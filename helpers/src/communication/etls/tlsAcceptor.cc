/**
 * @file tlsAcceptor.cpp
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.md'
 */

#include "tlsAcceptor.h"

#include "tlsApplication.h"

namespace one {
namespace communication {
namespace etls {

TLSAcceptor::TLSAcceptor(TLSApplication &app, const unsigned short port,
    const std::string &certPath, const std::string &keyPath,
    std::string rfc2818Hostname)
    : detail::WithSSLContext{asio::ssl::context::tlsv12_server, certPath,
          keyPath, std::move(rfc2818Hostname)}
    , m_app{app}
    , m_ioService{app.ioService()}
    , m_acceptor{m_ioService, {asio::ip::tcp::v4(), port}}
{
}

void TLSAcceptor::acceptAsync(Ptr self, Callback<TLSSocket::Ptr> callback)
{
    asio::post(m_ioService, [
        =, self = std::move(self), callback = std::move(callback)
    ]() mutable {
        auto sock = std::make_shared<TLSSocket>(m_app, m_context);
        m_acceptor.async_accept(sock->m_socket.lowest_layer(), [
            =, s = std::weak_ptr<TLSAcceptor>{self},
            callback = std::move(callback)
        ](const auto ec) mutable {
            if (ec) {
                callback(ec);
            }
            else if (auto self2 = s.lock()) {
                sock->m_socket.lowest_layer().set_option(
                    asio::ip::tcp::no_delay{true});

                callback(sock);
            }
            else {
                callback(std::make_error_code(std::errc::operation_canceled));
            }
        });
    });
}

void TLSAcceptor::localEndpointAsync(
    Ptr self, Callback<const asio::ip::tcp::endpoint &> callback)
{
    asio::post(m_ioService, [
        =, self = std::move(self), callback = std::move(callback)
    ]() mutable { callback(m_acceptor.local_endpoint()); });
}

} // namespace etls
} // namespace communication
} // namespace one
