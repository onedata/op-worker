/**
 * @file handshakeRequest.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "messages/handshakeRequest.h"

#include "client_messages.pb.h"

namespace one {
namespace messages {

std::unique_ptr<ProtocolClientMessage> HandshakeRequest::serialize() const
{
    auto clientMsg = std::make_unique<ProtocolClientMessage>();
    auto handshakeRequestMsg = clientMsg->mutable_handshake_request();
    auto tokenMsg = handshakeRequestMsg->mutable_token();
    handshakeRequestMsg->set_session_id(m_session_id);
    tokenMsg->set_value(m_token);

    return clientMsg;
}

} // namespace messages
} // namespace one
