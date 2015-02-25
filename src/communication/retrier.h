/**
 * @file retrier.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_RETRIER_H
#define HELPERS_COMMUNICATION_RETRIER_H

#include "exception.h"

#include <functional>
#include <future>
#include <memory>

using namespace std::literals::chrono_literals;

namespace one {
namespace communication {

constexpr int DEFAULT_RETRIES = 2;
constexpr auto DEFAULT_SEND_TIMEOUT = 5s;

/**
 * Retrier is responsible for retrying message send operation handled by
 * a lower layer.
 */
template <class LowerLayer> class Retrier {
public:
    /**
     * Creates an instance of @c Retrier that passes messages down to a lower
     * layer.
     * @param lowerLayer A pointer to an object implementing methods:
     * * <tt>std::future<void> send(std::string)</tt>
     * * @c setOnMessageCallback(std::function<void(std::string)>)
     */
    Retrier(decltype(m_lowerLayer) lowerLayer)
        : m_lowerLayer{std::move(lowerLayer)}
    {
        m_lowerLayer->setOnMessageCallback([this](std::string message) mutable {
            m_onMessageCallback(std::move(message));
        });
    }

    /**
     * Sets the callback called after the layer has processed a message.
     * @param onMessageCallback The callback to call.
     */
    void setOnMessageCallback(decltype(m_onMessage) onMessageCallback)
    {
        m_onMessageCallback = std::move(onMessageCallback);
    }

    /**
     * Sends a message with retries.
     * This method returns once a message is received by the remote endpoint,
     * or an error occurs and the number of retries has been exhausted.
     * If the message cannot be sent, last exception is thrown.
     * @param message The message to send.
     * @param retries The number of retries.
     */
    void send(std::string message, const int retries = DEFAULT_RETRIES)
    {
        std::future<void> future = m_lowerLayer->send(message);

        try {
            if (future.wait_for(DEFAULT_SEND_TIMEOUT) !=
                std::future_status::ready)
                throw SendError("timeout when waiting for results of send");

            future.get();
            return;
        }
        catch (SendError) {
            if (retries == 0)
                throw;

            send(std::move(message), retries - 1);
        }
    }

private:
    std::unique_ptr<LowerLayer> m_lowerLayer;
    std::function<void(std::string)> m_onMessageCallback = [](auto) {};
};

} // namespace one
} // namespace communication

#endif // HELPERS_COMMUNICATION_RETRIER_H
