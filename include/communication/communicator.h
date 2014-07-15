/**
 * @file communicator.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATOR_H
#define VEILHELPERS_COMMUNICATOR_H


#include "communication/communicationHandler.h"
#include "communication/messages.h"
#include "veilErrors.h"

#include <memory>
#include <string>

namespace veil
{
namespace communication
{

class CertificateData;

class Communicator
{
    using Answer = protocol::communication_protocol::Answer;

public:
    Communicator(const unsigned int dataConnectionsNumber,
                 const unsigned int metaConnectionsNumber,
                 std::shared_ptr<const CertificateData> certificateData,
                 const bool verifyServerCertificate);

    void enablePushChannel(std::function<void(const Answer&)> callback);

    bool sendHandshakeACK();

    void setFuseId(std::string fuseId);

    template<typename MsgType>
    std::future<std::unique_ptr<Answer>> communicate(const MsgType &msg)
    {
        const auto clusterMsg = messages::create(msg);
        const auto pool = messages::pool<MsgType>();
        return m_communicationHandler.communicate(*clusterMsg, pool);
    }

    template<typename MsgType>
    void send(const MsgType &msg)
    {
        const auto clusterMsg = messages::create(msg);
        const auto pool = messages::pool<MsgType>();
        return m_communicationHandler.send(*clusterMsg, pool);
    }

private:
    const std::string m_uri;
    CommunicationHandler m_communicationHandler;
    std::string m_fuseId;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATOR_H
