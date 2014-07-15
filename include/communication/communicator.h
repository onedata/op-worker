/**
 * @file communicator.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATOR_H
#define VEILHELPERS_COMMUNICATOR_H


#include "communication/communicationHandler.h"

#include <memory>
#include <string>

namespace veil
{
namespace communication
{

static constexpr int PROTOCOL_VERSION = 1;
static constexpr const char
    *FUSE_MESSAGES          = "fuse_messages",
    *COMMUNICATION_PROTOCOL = "communication_protocol",
    *LOGGING                = "logging",
    *REMOTE_FILE_MANAGEMENT = "remote_file_management";

static constexpr const char
    *CENTRAL_LOGGER_MODULE_NAME     = "central_logger";

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

private:
    //void registerPushChannel();

    const std::string m_uri;
    CommunicationHandler m_communicationHandler;
    std::string m_fuseId;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATOR_H
