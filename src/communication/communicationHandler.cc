/**
 * @file communicationHandler.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicationHandler.h"

#include "communication/connection.h"
#include "communication/connectionPool.h"
#include "make_unique.h"

#include <functional>
#include <numeric>

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
    const auto &pool = poolType == Pool::DATA ? m_dataPool : m_metaPool;
    message.set_message_id(nextId());

    std::promise<std::unique_ptr<CommunicationHandler::Answer>> promise;
    auto future = promise.get_future();
    m_promises.emplace(message.message_id(), std::move(promise));

    pool->send(message.SerializeAsString());

    return std::move(future);
}

void CommunicationHandler::subscribe(SubscriptionData data)
{
    m_subscriptions.emplace_front(std::move(data));
}

CommunicationHandler::MsgId CommunicationHandler::nextId()
{
    const MsgId m = ++m_nextMsgId;
    return m < 0 ? m - std::numeric_limits<MsgId>::min() : m;
}

void CommunicationHandler::onMessage(const std::string &payload)
{
    auto answer = std::make_unique<Answer>();
    answer->ParsePartialFromString(payload);

    for(auto it = m_subscriptions.begin(); it != m_subscriptions.end();)
    {
        if(it->predicate(*answer) && !it->callback(*answer))
            it = m_subscriptions.erase(it);
        else
            ++it;
    }

    const auto it = m_promises.find(answer->message_id());
    if(it != m_promises.end())
    {
        it->second.set_value(std::move(answer));
        m_promises.erase(it);
    }
}

CommunicationHandler::SubscriptionData::SubscriptionData(std::function<bool(const Answer&)> predicate,
                                                         std::function<bool(const Answer&)> callback)
    : predicate{std::move(predicate)}
    , callback{std::move(callback)}
{
}

} // namespace communication
} // namespace veil
