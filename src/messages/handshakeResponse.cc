/**
 * @file handshakeResponse.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "messages/handshakeResponse.h"

#include "server_messages.pb.h"

namespace one {
namespace messages {

HandshakeResponse::HandshakeResponse(
    std::unique_ptr<ProtocolServerMessage> serverMessage)
{
    auto &handshakeResponseMsg = serverMessage->handshake_response();
    m_session_id = handshakeResponseMsg.session_id();
}

} // namespace messages
} // namespace one
