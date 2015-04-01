/**
* @file handshakeResponse.h
* @author Krzysztof Trzepla
* @copyright (C) 2015 ACK CYFRONET AGH
* @copyright This software is released under the MIT license cited in
* 'LICENSE.txt'
*/

#ifndef HELPERS_MESSAGES_HANDSHAKE_RESPONSE_H
#define HELPERS_MESSAGES_HANDSHAKE_RESPONSE_H

#include "messages/serverMessage.h"

#include <memory>

namespace one {

namespace clproto {
class ClientMessage;
}

namespace messages {

/**
* The HandshakeResponse class represents a message that is sent by the server to
* confirm session establishment.
*/
class HandshakeResponse : public ServerMessage {
public:
    /**
    * Constructor.
    * @param serverMessage Protocol Buffers message representing @c
    * HandshakeResponse counterpart.
    */
    HandshakeResponse(std::unique_ptr<ProtocolServerMessage> serverMessage);

private:
    std::string m_session_id;
};

} // namespace messages
} // namespace one

#endif // HELPERS_MESSAGES_HANDSHAKE_RESPONSE_H
