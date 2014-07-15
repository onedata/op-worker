/**
 * @file communicator.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATOR_H
#define VEILHELPERS_COMMUNICATOR_H


#include "communication/communicationHandler.h"
#include "make_unique.h"
#include "veilErrors.h"

#include <chrono>
#include <memory>
#include <string>

namespace veil
{
namespace communication
{

static constexpr int PROTOCOL_VERSION = 1;

static constexpr const char
    *FUSE_MESSAGES_DECODER          = "fuse_messages",
    *COMMUNICATION_PROTOCOL_DECODER = "communication_protocol",
    *LOGGING_DECODER                = "logging",
    *REMOTE_FILE_MANAGEMENT_DECODER = "remote_file_management";

static constexpr const char
    *FSLOGIC_MODULE_NAME                = "fslogic",
    *CENTRAL_LOGGER_MODULE_NAME         = "central_logger",
    *REMOTE_FILE_MANAGEMENT_MODULE_NAME = "remote_files_manager";

static constexpr std::chrono::seconds RECV_TIMEOUT{2000};

class CertificateData;

class Communicator
{
    using Answer = protocol::communication_protocol::Answer;
    using ClusterMsg = protocol::communication_protocol::ClusterMsg;

public:
    Communicator(const unsigned int dataConnectionsNumber,
                 const unsigned int metaConnectionsNumber,
                 std::shared_ptr<const CertificateData> certificateData,
                 const bool verifyServerCertificate);

    void enablePushChannel(std::function<void(const Answer&)> callback);

    bool sendHandshakeACK();

    void setFuseId(std::string fuseId);

    void send(ClusterMsg &msg);
    std::future<std::unique_ptr<Answer>> communicateAsync(ClusterMsg &msg);
    std::unique_ptr<Answer> communicate(ClusterMsg &msg,
                                        const std::chrono::milliseconds timeout = RECV_TIMEOUT);

private:
    CommunicationHandler::Pool poolType(const ClusterMsg &msg) const;

    const std::string m_uri;
    CommunicationHandler m_communicationHandler;
    std::string m_fuseId;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATOR_H
