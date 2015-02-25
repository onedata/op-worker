/**
 * @file communicationHandler.h
 * @author Konrad Zemek
 * @copyright (C) 2014-2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_COMMUNICATION_HANDLER_H
#define HELPERS_COMMUNICATION_COMMUNICATION_HANDLER_H

#include "client_messages.pb.h"
#include "server_messages.pb.h"
#include "subscriptionData.h"

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_queue.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace one {
namespace communication {

/**
 * The CommunicationHandler class is responsible for managing communication on
 * the layer of @c one::clproto::communication_protocol::ClusterMsg and
 * @c one::clproto::communication_protocol::Answer messages.
 * The CommunicationHandler uses @c one::communication::ConnectionPool as the
 * underlying layer and manages multiple @c ConnectionPool instances allowing
 * clients to send messages through any of them.
 */
template <class LowerLayer> class CommunicationHandler {
    using ServerMessage = clproto::ServerMessage;
    using ClientMessage = clproto::ClientMessage;
    using ServerMessagePtr = std::unique_ptr<clproto::ServerMessage>;

public:
    /**
     * The Pool enum enumerates possible @c one::communication::ConnectionPool
     * instances for message sending purposes.
     */
    enum class Pool { META, DATA };

    /**
     * Constructor.
     * @param dataPool A connection pool to use for sending file data.
     * @param metaPool A connection pool to use for sending metadata.
     */
    CommunicationHandler(std::unique_ptr<LowerLayer> dataPool,
        std::unique_ptr<LowerLayer> metaPool)
        : m_dataPool{std::move(dataPool)}
        , m_metaPool{std::move(metaPool)}
    {
        m_dataPool->setOnMessageCallback(
            std::bind(&CommunicationHandler<LowerLayer>::onMessage, this, _1));

        m_metaPool->setOnMessageCallback(
            std::bind(&CommunicationHandler<LowerLayer>::onMessage, this, _1));
    }

    virtual ~CommunicationHandler() = default;

    /**
     * Sends a message as a reply to a message from the server.
     * @param originalMsg The message to reply to.
     * @param replyMsg The reply to be sent.
     * @param poolType Type of a pool through which the reply should be sent.
     * @param retries Number of retries in case of sending error.
     */
    virtual void reply(const ServerMessage &originalMsg,
        ClientMessage &replyMsg, const Pool poolType,
        const unsigned int retries)
    {
        DLOG(INFO) << "Replying to message (id: " << originalMsg->message_id()
                   << ") through "
                   << (poolType == Pool::DATA ? "data" : "metadata") << " pool";

        replyMsg.set_message_id(originalMsg.message_id());
        send(replyMsg, poolType, retries);
    }

    /**
     * Sends a message to the server.
     * @param message The message to be sent.
     * @param poolType Type of a pool through which the message should be sent.
     * @param retries Number of retries in case of sending error.
     */
    virtual void send(const ClientMessage &message, const Pool poolType,
        const unsigned int retries)
    {
        DLOG(INFO) << "Sending a message through "
                   << (poolType == Pool::DATA ? "data" : "metadata") << " pool";

        const auto &pool = poolType == (Pool::DATA ? m_dataPool : m_metaPool);
        pool->send(message, retries);
    }

    /**
     * Sends a message to the server and sets up to receive a reply.
     * @param message The message to be sent.
     * @param poolType Type of a pool through which the message should be sent.
     * @param retries Number of retries in case of sending error.
     * @return A future which should be fulfiled with server's reply.
     */
    virtual std::future<std::unique_ptr<ServerMessage>> communicate(
        ClientMessage &message, const Pool poolType, const unsigned int retries)
    {
        std::lock_guard<std::mutex> guard{m_promisesMutex};
        message.set_message_id(nextId());

        auto promise = std::make_shared<std::promise<ServerMessagePtr>>();
        auto future = promise->get_future();

        decltype(m_promises)::accessor acc;
        m_promises.insert(acc, message.message_id());
        acc->second = std::move(promise);
        acc.release();

        send(replyMsg, poolType, retries);
        return future;
    }

    /**
     * Subscribes a given callback to messages received from the server.
     * Message can be received through any connection.
     * @note Subscription callbacks should be lightweight, as they are handled
     * in communicator's threads.
     * The @c SubscriptionData::predicate is first called to determine if
     * the @c SubscriptionData::callback should be called.
     * @param data A structure holding the predicate and callback functions.
     * @return A function to cancel the subscription.
     */
    virtual std::function<void()> subscribe(SubscriptionData data)
    {
        decltype(m_subscriptions)::iterator it;
        if (m_unusedSubscriptions.try_pop(it))
            *it = std::move(data);
        else
            m_subscriptions.emplace_back(std::move(data));

        return [this, it] {
            *it = SubscriptionData{};
            m_unusedSubscriptions.push(it);
        };
    }

private:
    void onMessage(ServerMessagePtr message)
    {
        decltype(m_promises)::accessor acc;
        const bool handled = m_promises.find(acc, message->message_id());

        for (const auto &sub : m_subscriptions)
            if (sub.predicate(*message, handled))
                sub.callback(*message);

        if (handled) {
            auto promise = std::move(*acc->second);
            m_promises.erase(acc);
            promise.set_value(std::move(message));
        }
    }

    std::string nextId() { return std::to_string(++m_nextMsgId); }

    const std::unique_ptr<ConnectionPool> m_dataPool;
    const std::unique_ptr<ConnectionPool> m_metaPool;

    tbb::concurrent_hash_map<std::string,
        std::shared_ptr<std::promise<ServerMessagePtr>>> m_promises;

    /// The counter will loop after sending ~65000 messages, providing us with
    /// a natural size bound for m_promises.
    std::atomic<std::uint16_t> m_nextMsgId{0};

    tbb::concurrent_vector<SubscriptionData> m_subscriptions;
    tbb::concurrent_queue<decltype<m_subscriptions>::iterator>
        m_unusedSubscriptions;
};

} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_COMMUNICATION_HANDLER_H
