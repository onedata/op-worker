/**
 * @file translator.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_TRANSLATOR_H
#define HELPERS_COMMUNICATION_TRANSLATOR_H

#include "logging.h"
#include "client_messages.pb.h"
#include "server_messages.pb.h"

#include <functional>
#include <memory>

namespace one {
namespace communication {

/**
 * Translator is responsible for translating @c ClientMessage protobuf messages
 * to string and string to @c ServerMessage protobuf messages.
 */
template <class LowerLayer> class Translator {
public:
    /**
     * Creates an instance of @c Translator that passes messages down to a lower
     * layer.
     * @param lowerLayer A pointer to an object implementing methods:
     * * @c send(std::string)
     * * @c setOnMessageCallback(std::function<void(std::string)>)
     */
    Translator(decltype(m_lowerLayer) lowerLayer)
        : m_lowerLayer{std::move(lowerLayer)}
    {
        m_lowerLayer->setOnMessageCallback(
            std::bind(&Translator<LowerLayer>::onMessage, this));
    }

    /**
     * Sets the callback called after the layer has processed a message.
     * @param onMessageCallback The callback to call.
     */
    void setOnMessageCallback(decltype(m_onMessageCallback) onMessageCallback)
    {
        m_onMessageCallback = std::move(onMessageCallback);
    }

    /**
     * Serializes an instance of @c clproto::ClientMessage as @c std::string
     * and passes it down to the lower layer.
     * @param message The message to send.
     */
    decltype(m_lowerLayer->send) send(const clproto::ClientMessage &message)
    {
        return m_lowerLayer->send(message.SerializeAsString());
    }

private:
    void onMessage(std::string message)
    {
        auto serverMsg = std::make_unique<clproto::ServerMessage>();
        if (serverMsg->ParseFromString(message))
            m_onMessageCallback(std::move(serverMessage));
        else
            DLOG(WARNING) << "Received an invalid message from the server: '"
                          << payload.substr(0, 40)
                          << "' (message trimmed to 40 chars).";
    }

    std::unique_ptr<LowerLayer> m_lowerLayer;
    std::function<void(std::unique_ptr<clproto::ServerMessage>)>
        m_onMessageCallback = [](auto) {};
};

} // namespace one
} // namespace communication

#endif // HELPERS_COMMUNICATION_TRANSLATOR_H
