/**
 * @file communicationHandler.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATION_HANDLER_H
#define VEILHELPERS_COMMUNICATION_HANDLER_H


#include "communication_protocol.pb.h"

#include <functional>
#include <future>
#include <memory>

namespace veil
{
namespace communication
{

class ConnectionPool;

class CommunicationHandler
{
    using Answer = protocol::communication_protocol::Answer;
    using Message = protocol::communication_protocol::ClusterMsg;

public:
    CommunicationHandler(std::unique_ptr<ConnectionPool> dataPool,
                         std::unique_ptr<ConnectionPool> metaPool);

    CommunicationHandler(const CommunicationHandler&) = delete;
    CommunicationHandler &operator=(const CommunicationHandler&) = delete;

    std::future<Answer> send();

    void subscribe(std::function<bool(const Answer&)> predicate,
                   std::function<void(const Answer&)> callback);

    void setFuseId(std::string fuseId);

    void registerPushChannels();

private:
    const std::unique_ptr<ConnectionPool> m_dataPool;
    const std::unique_ptr<ConnectionPool> m_metaPool;
    std::string m_fuseId;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATION_HANDLER_H
