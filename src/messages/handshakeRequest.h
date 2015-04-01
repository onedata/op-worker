/**
* @file handshakeRequest.h
* @author Krzysztof Trzepla
* @copyright (C) 2015 ACK CYFRONET AGH
* @copyright This software is released under the MIT license cited in
* 'LICENSE.txt'
*/

#ifndef HELPERS_MESSAGES_HANDSHAKE_REQUEST_H
#define HELPERS_MESSAGES_HANDSHAKE_REQUEST_H

#include "messages/clientMessage.h"

#include <memory>

namespace one {

namespace clproto {
class ClientMessage;
}

namespace messages {

/**
* The HandshakeRequest class represents a message that is sent by the client to
* establish session.
*/
class HandshakeRequest : public ClientMessage {
public:
    virtual std::unique_ptr<ProtocolClientMessage> serialize() const override;

private:
    std::string m_session_id;
    std::string m_token;
};

} // namespace messages
} // namespace one

#endif // HELPERS_MESSAGES_HANDSHAKE_REQUEST_H
