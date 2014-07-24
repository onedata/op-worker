/**
 * @file communicator.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicator.h"

#include "communication/connection.h"
#include "communication/exception.h"
#include "fuse_messages.pb.h"
#include "logging.h"
#include "make_unique.h"
#include "veilErrors.h"
#include "remote_file_management.pb.h"

#include <boost/algorithm/string.hpp>
#include <google/protobuf/descriptor.h>

#include <cassert>
#include <unordered_set>

namespace veil
{
namespace communication
{

Communicator::Communicator(std::unique_ptr<CommunicationHandler> communicationHandler)
    : m_communicationHandler{std::move(communicationHandler)}
{
}

void Communicator::setupPushChannels(std::function<void(const Answer&)> callback)
{
    LOG(INFO) << "Setting up push channels with fuseId: '" << m_fuseId << "'";

    // First close the push channel that was already opened on the connection
    static std::function<void()> deregisterPushChannel;
    if(deregisterPushChannel)
        deregisterPushChannel();

    // Subscribe for push messages (negative message id)
    auto pred = [](const Answer &ans){ return ans.message_id() < 0; };
    auto unsubscribe = m_communicationHandler->subscribe({std::move(pred),
                                                          std::move(callback)});

    const auto fuseId = m_fuseId;

    auto handshake = [this, fuseId]{
        LOG(INFO) << "Opening a push channel with fuseId: '" << fuseId << "'";
        protocol::fuse_messages::ChannelRegistration reg;
        reg.set_fuse_id(fuseId);
        return createMessage(FSLOGIC_MODULE_NAME, true, Atom::default_instance(), reg);
    };

    auto goodbye = [this, fuseId]{
        LOG(INFO) << "Closing the push channel with fuseId: '" << fuseId << "'";
        protocol::fuse_messages::ChannelClose close;
        close.set_fuse_id(fuseId);
        return createMessage(FSLOGIC_MODULE_NAME, true, Atom::default_instance(), close);
    };

    auto remove = m_communicationHandler->addHandshake(std::move(handshake),
                                                       std::move(goodbye),
                                                       CommunicationHandler::Pool::META);

    deregisterPushChannel = [=]{ unsubscribe(); remove(); };
}

void Communicator::setupHandshakeAck()
{
    LOG(INFO) << "Enabling HandshakeAck with fuseId: '" << m_fuseId << "'";

    // First remove the previous Ack message
    static std::function<void()> removeHandshakeAck;
    if(removeHandshakeAck)
        removeHandshakeAck();

    const auto fuseId = m_fuseId;

    // Build HandshakeAck message
    auto handshake = [this, fuseId]{
        LOG(INFO) << "Sending HandshakeAck with fuseId: '" << fuseId << "'";
        protocol::fuse_messages::HandshakeAck ack;
        ack.set_fuse_id(fuseId);
        return createMessage("", true, Atom::default_instance(), ack);
    };

    auto removeMeta = m_communicationHandler->addHandshake(handshake, CommunicationHandler::Pool::META);
    auto removeData = m_communicationHandler->addHandshake(handshake, CommunicationHandler::Pool::DATA);
    removeHandshakeAck = [=]{ removeMeta(); removeData(); };
}

void Communicator::setFuseId(std::string fuseId)
{
    m_fuseId = std::move(fuseId);
}

void Communicator::reply(const Answer &originalMsg, const std::string &module,
                         const google::protobuf::Message &msg)
{
    auto cmsg = createMessage(module, false,
                              veil::protocol::communication_protocol::Atom::default_instance(),
                              msg);
    m_communicationHandler->reply(originalMsg, *cmsg, poolType(msg));
}

void Communicator::send(const std::string &module,
                        const google::protobuf::Message &msg)
{
    auto cmsg = createMessage(module, false,
                              veil::protocol::communication_protocol::Atom::default_instance(),
                              msg);
    m_communicationHandler->send(*cmsg, poolType(msg));
}

std::future<std::unique_ptr<Communicator::Answer>>
Communicator::communicateAsync(const std::string &module,
                               const google::protobuf::Message &msg,
                               const google::protobuf::Message &ans)
{
    auto cmsg = createMessage(module, false, ans, msg);
    return m_communicationHandler->communicate(*cmsg, poolType(msg));
}

std::unique_ptr<Communicator::Answer>
Communicator::communicate(const std::string &module,
                          const google::protobuf::Message &msg,
                          const google::protobuf::Message &ans,
                          const std::chrono::milliseconds timeout)
{
    auto cmsg = createMessage(module, true, ans, msg);
    auto future = m_communicationHandler->communicate(*cmsg, poolType(msg));

    if(future.wait_for(timeout) != std::future_status::ready)
        throw ReceiveError{"timeout of " + std::to_string(timeout.count()) +
                          " milliseconds exceeded"};

    return future.get();
}

std::pair<std::string, std::string>
Communicator::describe(const google::protobuf::Descriptor &desc) const
{
    std::vector<std::string> pieces;
    boost::algorithm::split(pieces, desc.full_name(), boost::algorithm::is_any_of("."));
    assert(pieces.size() >= 2);

    const auto nameIt = pieces.rbegin();
    const auto decoderIt = pieces.rbegin() + 1;

    boost::algorithm::to_lower(*nameIt);
    boost::algorithm::to_lower(*decoderIt);

    return {std::move(*nameIt), std::move(*decoderIt)};
}

std::unique_ptr<Communicator::ClusterMsg>
Communicator::createMessage(const std::string &module,
                            const bool synchronous,
                            const google::protobuf::Message &ans,
                            const google::protobuf::Message &msg) const
{
    const auto msgInfo = describe(*msg.GetDescriptor());
    const auto ansInfo = describe(*ans.GetDescriptor());

    auto cmsg = std::make_unique<ClusterMsg>();
    cmsg->set_protocol_version(PROTOCOL_VERSION);
    cmsg->set_module_name(module);
    cmsg->set_message_type(msgInfo.first);
    cmsg->set_message_decoder_name(msgInfo.second);
    cmsg->set_answer_type(ansInfo.first);
    cmsg->set_answer_decoder_name(ansInfo.second);
    cmsg->set_synch(synchronous);
    msg.SerializeToString(cmsg->mutable_input());

    return std::move(cmsg);
}

CommunicationHandler::Pool Communicator::poolType(const google::protobuf::Message &msg) const
{
    static const std::unordered_set<std::string> dataPoolMessages{
        describe(*protocol::remote_file_management::RemoteFileMangement::descriptor()).second
    };

    return dataPoolMessages.count(describe(*msg.GetDescriptor()).second)
            ? CommunicationHandler::Pool::DATA
            : CommunicationHandler::Pool::META;
}

} // namespace communication
} // namespace veil
