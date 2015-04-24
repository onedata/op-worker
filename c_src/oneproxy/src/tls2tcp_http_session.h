/**
 * @file tls2tcp_http_session.h
 * @author Rafal Slota
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef TLS2TCP_HTTP_SESSION_H
#define TLS2TCP_HTTP_SESSION_H

#include "tls2tcp_session.h"

namespace one {
namespace proxy {

/**
 * The tls2tcp_http_session class is a special case of tls2tcp_session, where
 * the proxy will inject client's information into the first HTTP packet.
 */
class tls2tcp_http_session : public tls2tcp_session {
public:
    using tls2tcp_session::tls2tcp_session;

private:
    /**
     * Injects client information into a HTTP packet and deferrs control to
     * the base class.
     * @param verified The result of OpenSSL peer certificate verification.
     */
    virtual void post_handshake(const bool verified) override;
};

} // namespace proxy
} // namespace one

#endif // TLS2TCP_HTTP_SESSION_H
