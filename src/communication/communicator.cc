/**
 * @file communicator.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicator.h"

#include "communication_protocol.pb.h"
#include "communication/connection.h"
#include "communication/websocketConnectionPool.h"
#include "fuse_messages.pb.h"
#include "logging.h"
#include "make_unique.h"

#include <boost/algorithm/string/case_conv.hpp>

using boost::algorithm::to_lower_copy;
using namespace veil::protocol::fuse_messages;
using namespace veil::protocol::communication_protocol;

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
    Message msg;
    ChannelRegistration reg;
    reg.set_fuse_id(m_fuseId);

    msg.set_module_name("fslogic");
    msg.set_protocol_version(PROTOCOL_VERSION);
    msg.set_message_type(to_lower_copy(reg.GetDescriptor()->name()));
    msg.set_message_decoder_name(to_lower_copy(std::string{FUSE_MESSAGES}));
    msg.set_answer_type(to_lower_copy(Atom::descriptor()->name()));
    msg.set_answer_decoder_name(to_lower_copy(std::string{COMMUNICATION_PROTOCOL}));
    msg.set_synch(true);
    reg.SerializeToString(msg.mutable_input());

    m_communicationHandler.addHandshake(msg, CommunicationHandler::Pool::META);
}

} // namespace communication
} // namespace veil
