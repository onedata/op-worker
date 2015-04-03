/**
 * @file tls2tcp_cert_aware_session.cpp
 * @author Tomasz Lichon
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef TLS2TCP_CERT_AWARE_SESSION_H
#define TLS2TCP_CERT_AWARE_SESSION_H

#include "tls2tcp_session.h"

namespace one {
namespace proxy {

/**
 * The tls2tcp_cert_aware_session class is a special case of tls2tcp_session, where
 * the proxy, will send CertInfo message to server, after successful handshake
 */
class tls2tcp_cert_aware_session : public tls2tcp_session {
public:
    using tls2tcp_session::tls2tcp_session;

private:
    /**
     * Sends CertInfo message to server and deferrs control to
     * the base class.
     * @param verified The result of OpenSSL peer certificate verification.
     */
    virtual void post_handshake(const bool verified) override;
};

} // namespace proxy
} // namespace one

#endif // TLS2TCP_CERT_AWARE_SESSION_H
