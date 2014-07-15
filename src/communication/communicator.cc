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

#include <boost/algorithm/string/case_conv.hpp>

#include <unordered_set>

namespace
{
std::string lower(std::string what)
{
    boost::algorithm::to_lower(what);
    return std::move(what);
}
}

namespace veil
{
namespace communication
{

Communicator::Communicator(const unsigned int dataConnectionsNumber,
                           const unsigned int metaConnectionsNumber,
                           std::shared_ptr<const CertificateData> certificateData,
                           const bool verifyServerCertificate)
    : m_uri{"wss://whatever"}
    , m_communicationHandler{
        std::make_unique<WebsocketConnectionPool>(
              dataConnectionsNumber, m_uri, certificateData, verifyServerCertificate),
        std::make_unique<WebsocketConnectionPool>(
              metaConnectionsNumber, m_uri, certificateData, verifyServerCertificate)}
{
}

void Communicator::enablePushChannel(std::function<void(const Answer&)> callback)
{
    assert(!m_fuseId.empty());

    LOG(INFO) << "Sending registerPushChannel request with FuseId: " << m_fuseId;

    const auto pred = [](const Answer &ans){ return ans.message_id() < 0; };
    const auto call = [=](const Answer &ans){ callback(ans); return true; };
    m_communicationHandler.subscribe({std::move(pred), std::move(call)});

    // Prepare PUSH channel registration request message
    protocol::fuse_messages::ChannelRegistration reg;
    protocol::fuse_messages::ChannelClose close;
    reg.set_fuse_id(m_fuseId);
    close.set_fuse_id(m_fuseId);

    ClusterMsg genericMsg;
    genericMsg.set_module_name(FSLOGIC_MODULE_NAME);
    genericMsg.set_protocol_version(PROTOCOL_VERSION);
    genericMsg.set_message_decoder_name(FUSE_MESSAGES_DECODER);
    genericMsg.set_answer_type(lower(protocol::communication_protocol::Atom::descriptor()->name()));
    genericMsg.set_answer_decoder_name(COMMUNICATION_PROTOCOL_DECODER);
    genericMsg.set_synch(true);

    ClusterMsg handshakeMsg = genericMsg;
    handshakeMsg.set_message_type(lower(reg.GetDescriptor()->name()));
    reg.SerializeToString(handshakeMsg.mutable_input());

    ClusterMsg goodbyeMsg = genericMsg;
    goodbyeMsg.set_message_type(lower(close.GetDescriptor()->name()));
    close.SerializeToString(handshakeMsg.mutable_input());

    m_communicationHandler.addHandshake(handshakeMsg, goodbyeMsg,
                                        CommunicationHandler::Pool::META);
}

bool Communicator::sendHandshakeACK()
{
    assert(!m_fuseId.empty());

    LOG(INFO) << "Sending HandshakeAck with fuseId: '" << m_fuseId << "'";

    // Build HandshakeAck message
    protocol::fuse_messages::HandshakeAck ack;
    ack.set_fuse_id(m_fuseId);

    ClusterMsg msg;
    msg.set_module_name("");
    msg.set_protocol_version(PROTOCOL_VERSION);
    msg.set_message_type(lower(ack.GetDescriptor()->name()));
    msg.set_message_decoder_name(FUSE_MESSAGES_DECODER);
    msg.set_answer_type(lower(protocol::communication_protocol::Atom::descriptor()->name()));
    msg.set_answer_decoder_name(COMMUNICATION_PROTOCOL_DECODER);
    msg.set_synch(true);
    ack.SerializeToString(msg.mutable_input());

    // Send HandshakeAck to cluster
    return communicate(msg)->answer_status() == VOK;
}

void Communicator::setFuseId(std::string fuseId)
{
    m_fuseId = std::move(fuseId);
}

std::future<std::unique_ptr<Communicator::Answer>>
Communicator::communicateAsync(ClusterMsg &msg)
{
    return m_communicationHandler.communicate(msg, poolType(msg));
}

std::unique_ptr<Communicator::Answer>
Communicator::communicate(ClusterMsg &msg,
                          const std::chrono::milliseconds timeout)
{
    auto future = communicateAsync(msg);
    return future.wait_for(timeout) == std::future_status::ready
            ? future.get() : std::make_unique<Answer>();
}

void Communicator::send(Communicator::ClusterMsg &msg)
{
    m_communicationHandler.communicate(msg, poolType(msg));
}

CommunicationHandler::Pool Communicator::poolType(const ClusterMsg &msg) const
{
    static const std::unordered_set<std::string> dataPoolMessages{
        lower(veil::protocol::remote_file_management::RemoteFileMangement::descriptor()->name())
    };

    return dataPoolMessages.count(msg.message_type())
            ? CommunicationHandler::Pool::DATA
            : CommunicationHandler::Pool::META;
}

} // namespace communication
} // namespace veil
