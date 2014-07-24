/**
 * @file communicationHandler.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicationHandler.h"

#include "communication/connection.h"
#include "communication/connectionPool.h"
#include "logging.h"
#include "make_unique.h"

#include <functional>
#include <numeric>

static constexpr unsigned int MAX_PENDING_PROMISES = 10000;
static constexpr unsigned int PENDING_PROMISES_CUTOFF = 5000;

namespace veil
{
namespace communication
{

CommunicationHandler::CommunicationHandler(std::unique_ptr<ConnectionPool> dataPool,
                                           std::unique_ptr<ConnectionPool> metaPool)
    : m_dataPool{std::move(dataPool)}
    , m_metaPool{std::move(metaPool)}
{
    namespace p = std::placeholders;
    m_dataPool->setOnMessageCallback(std::bind(&CommunicationHandler::onMessage, this, p::_1));
    m_metaPool->setOnMessageCallback(std::bind(&CommunicationHandler::onMessage, this, p::_1));
}

void CommunicationHandler::reply(const Answer &originalMsg,
                                 Message &replyMsg, const Pool poolType)
{
    const auto &pool = poolType == Pool::DATA ? m_dataPool : m_metaPool;
    replyMsg.set_message_id(originalMsg.message_id());
    pool->send(replyMsg.SerializeAsString());
}

void
CommunicationHandler::send(Message &message, const Pool poolType)
{
    const auto &pool = poolType == Pool::DATA ? m_dataPool : m_metaPool;
    message.set_message_id(nextId());
    pool->send(message.SerializeAsString());
}

std::future<std::unique_ptr<CommunicationHandler::Answer>>
CommunicationHandler::communicate(Message &message, const Pool poolType)
{
    std::lock_guard<std::mutex> guard{m_promisesMutex};

    const auto &pool = poolType == Pool::DATA ? m_dataPool : m_metaPool;
    message.set_message_id(nextId());

    std::promise<std::unique_ptr<CommunicationHandler::Answer>> promise;
    auto future = promise.get_future();
    m_promises[message.message_id()] = std::move(promise);

    if(m_promises.size() >= MAX_PENDING_PROMISES)
    {
        LOG(WARNING) << "Pending message promises number exceeded " <<
                        MAX_PENDING_PROMISES << ". Cleaning up.";

        // Cleanup messages starting with the oldest: starting from the ones
        // with higher ID than the last sent message, and finishing with the
        // ones with lowest IDs if any still need cleaning up.
        auto it = m_promises.upper_bound(message.message_id());
        while(it != m_promises.end() && m_promises.size() > PENDING_PROMISES_CUTOFF)
            it = m_promises.erase(it);

        while(m_promises.size() > PENDING_PROMISES_CUTOFF)
            m_promises.erase(m_promises.begin());
    }

    pool->send(message.SerializeAsString());

    return std::move(future);
}

std::function<void()> CommunicationHandler::subscribe(SubscriptionData data)
{
    auto it = m_subscriptions.emplace(m_subscriptions.begin(), std::move(data));
    return [=]{ m_subscriptions.erase(it); };
}

std::function<void()> CommunicationHandler::addHandshake(std::function<std::unique_ptr<Message>()> handshake,
                                                         std::function<std::unique_ptr<Message>()> goodbye,
                                                         const Pool poolType)
{
    auto h = [=]{ auto m = handshake(); m->set_message_id(nextId()); return m->SerializeAsString(); };
    auto g = [=]{ auto m = goodbye(); m->set_message_id(nextId()); return m->SerializeAsString(); };

    const auto &pool = poolType == Pool::DATA ? m_dataPool : m_metaPool;
    return pool->addHandshake(std::move(h), std::move(g));
}

std::function<void()> CommunicationHandler::addHandshake(std::function<std::unique_ptr<Message>()> handshake,
                                                         const Pool poolType)
{
    auto h = [=]{ auto m = handshake(); m->set_message_id(nextId()); return m->SerializeAsString(); };

    const auto &pool = poolType == Pool::DATA ? m_dataPool : m_metaPool;
    return pool->addHandshake(std::move(h));
}

CommunicationHandler::MsgId CommunicationHandler::nextId()
{
    const MsgId m = ++m_nextMsgId;
    return m < 0 ? m - std::numeric_limits<MsgId>::min() : m;
}

void CommunicationHandler::onMessage(const std::string &payload)
{
    std::lock_guard<std::mutex> guard{m_promisesMutex};

    auto answer = std::make_unique<Answer>();
    answer->ParsePartialFromString(payload);

    for(const auto &sub: m_subscriptions)
        if(sub.predicate(*answer))
            sub.callback(*answer);

    const auto it = m_promises.find(answer->message_id());
    if(it != m_promises.end())
    {
        it->second.set_value(std::move(answer));
        m_promises.erase(it);
    }
}

CommunicationHandler::SubscriptionData::SubscriptionData(std::function<bool(const Answer&)> predicate,
                                                         std::function<void(const Answer&)> callback)
    : predicate(std::move(predicate))
    , callback(std::move(callback))
{
}

} // namespace communication
} // namespace veil
