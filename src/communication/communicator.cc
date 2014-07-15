/**
 * @file communicator.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicator.h"

#include "communication/connection.h"
#include "communication/messages.h"
#include "communication/websocketConnectionPool.h"
#include "logging.h"
#include "make_unique.h"
#include "veilErrors.h"

#include <boost/algorithm/string/case_conv.hpp>

using namespace veil::protocol::fuse_messages;
using namespace veil::protocol::communication_protocol;

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
    ChannelRegistration reg;
    reg.set_fuse_id(m_fuseId);

    m_communicationHandler.addHandshake(*messages::create(reg),
                                        CommunicationHandler::Pool::META);
}

bool Communicator::sendHandshakeACK()
{
    assert(!m_fuseId.empty());

    LOG(INFO) << "Sending HandshakeAck with fuseId: '" << m_fuseId << "'";

    // Build HandshakeAck message
    HandshakeAck ack;
    ack.set_fuse_id(m_fuseId);

    // Send HandshakeAck to cluster
    auto ans = m_communicationHandler.communicate(*messages::create(ack),
                                                  CommunicationHandler::Pool::META);

    return ans.get()->answer_status() == VOK;
}

} // namespace communication
} // namespace veil
