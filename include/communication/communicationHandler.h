/**
 * @file communicationHandler.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATION_COMMUNICATION_HANDLER_H
#define VEILHELPERS_COMMUNICATION_COMMUNICATION_HANDLER_H


#include "communication_protocol.pb.h"
#include "communication/connectionPool.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <unordered_map>

namespace veil
{
namespace communication
{

class ConnectionPool;

class CommunicationHandler
{
    using MsgId = int32_t;
    using Answer = protocol::communication_protocol::Answer;
    using Message = protocol::communication_protocol::ClusterMsg;

public:
    struct SubscriptionData
    {
        SubscriptionData(std::function<bool(const Answer&)> predicate,
                         std::function<bool(const Answer&)> callback);

        const std::function<bool(const Answer&)> predicate;
        const std::function<bool(const Answer&)> callback;
    };

    enum class Pool
    {
        META,
        DATA
    };

    CommunicationHandler(std::unique_ptr<ConnectionPool> dataPool,
                         std::unique_ptr<ConnectionPool> metaPool);

    virtual ~CommunicationHandler() = default;

    CommunicationHandler(CommunicationHandler&&) = delete;
    CommunicationHandler &operator=(CommunicationHandler&&) = delete;
    CommunicationHandler(const CommunicationHandler&) = delete;
    CommunicationHandler &operator=(const CommunicationHandler&) = delete;

    virtual void reply(const Answer &originalMsg, Message &replyMsg,
                       const Pool poolType);
    virtual void send(Message &message, const Pool poolType);
    virtual std::future<std::unique_ptr<Answer>> communicate(Message &message,
                                                             const Pool poolType);

    virtual void subscribe(SubscriptionData data);
    virtual void addHandshake(std::function<std::unique_ptr<Message>()> handshake,
                              const Pool poolType);
    virtual void addHandshake(std::function<std::unique_ptr<Message>()> handshake,
                              std::function<std::unique_ptr<Message>()> goodbye,
                              const Pool poolType);

private:
    MsgId nextId();
    void onMessage(const std::string &payload);

    const std::unique_ptr<ConnectionPool> m_dataPool;
    const std::unique_ptr<ConnectionPool> m_metaPool;
    std::atomic<MsgId> m_nextMsgId{0};
    std::unordered_map<MsgId, std::promise<std::unique_ptr<Answer>>> m_promises;
    std::list<SubscriptionData> m_subscriptions;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATION_COMMUNICATION_HANDLER_H
