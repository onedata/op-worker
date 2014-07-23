/**
 * @file communicator.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicator.h"

#include "communication/connection.h"
#include "communication/websocketConnectionPool.h"
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

Communicator::Communicator(std::unique_ptr<CommunicationHandler> communicationHandler,
                           std::string uri)
    : m_uri{std::move(uri)}
    , m_communicationHandler{std::move(communicationHandler)}
{
}

void Communicator::enablePushChannel(std::function<void(const Answer&)> callback)
{
    assert(!m_fuseId.empty());

    LOG(INFO) << "Sending registerPushChannel request with FuseId: " << m_fuseId;

    auto pred = [](const Answer &ans){ return ans.message_id() < 0; };
    auto call = [=](const Answer &ans){ callback(ans); return true; };
    m_communicationHandler->subscribe({std::move(pred), std::move(call)});

    // Prepare PUSH channel registration request message
    protocol::fuse_messages::ChannelRegistration reg;
    protocol::fuse_messages::ChannelClose close;
    reg.set_fuse_id(m_fuseId);
    close.set_fuse_id(m_fuseId);

    const auto handshakeMsg = createMessage(FSLOGIC_MODULE_NAME, true,
                                            Atom::default_instance(), reg);

    const auto goodbyeMsg = createMessage(FSLOGIC_MODULE_NAME, true,
                                          Atom::default_instance(), close);

    m_communicationHandler->addHandshake(*handshakeMsg, *goodbyeMsg,
                                         CommunicationHandler::Pool::META);
}

void Communicator::enableHandshakeAck()
{
    LOG(INFO) << "Enabling HandshakeAck with fuseId: '" << m_fuseId << "'";

    // Build HandshakeAck message
    protocol::fuse_messages::HandshakeAck ack;
    ack.set_fuse_id(m_fuseId);

    const auto handshakeMsg = createMessage("", true,
                                            Atom::default_instance(), ack);

    m_communicationHandler->addHandshake(*handshakeMsg, CommunicationHandler::Pool::META);
    m_communicationHandler->addHandshake(*handshakeMsg, CommunicationHandler::Pool::DATA);
}

void Communicator::setFuseId(std::string fuseId)
{
    m_fuseId = std::move(fuseId);
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
    return future.wait_for(timeout) == std::future_status::ready
            ? future.get() : std::make_unique<Answer>();
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
