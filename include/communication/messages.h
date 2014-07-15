/**
 * @file messages.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_MESSAGES_H
#define VEILHELPERS_MESSAGES_H


#include "communication_protocol.pb.h"
#include "communicationHandler.h"
#include "fuse_messages.pb.h"
#include "logging.pb.h"
#include "remote_file_management.pb.h"

#include <memory>

namespace veil
{
namespace communication
{
namespace messages
{

static constexpr int PROTOCOL_VERSION = 1;
static constexpr const char
    *FUSE_MESSAGES_DECODER          = "fuse_messages",
    *COMMUNICATION_PROTOCOL_DECODER = "communication_protocol",
    *LOGGING_DECODER                = "logging",
    *REMOTE_FILE_MANAGEMENT_DECODER = "remote_file_management";

static constexpr const char
    *FSLOGIC_MODULE_NAME            = "fslogic",
    *CENTRAL_LOGGER_MODULE_NAME     = "central_logger";

using namespace veil::protocol::communication_protocol;
using namespace veil::protocol::fuse_messages;
using namespace veil::protocol::logging;
using namespace veil::protocol::remote_file_management;

std::unique_ptr<ClusterMsg> create(const ChannelRegistration &msg);
std::unique_ptr<ClusterMsg> create(const ChannelClose        &msg);
std::unique_ptr<ClusterMsg> create(const HandshakeAck        &msg);

template<class T>
constexpr CommunicationHandler::Pool pool()
{
    return CommunicationHandler::Pool::META;
}

template<>
constexpr CommunicationHandler::Pool pool<RemoteFileMangement>()
{
    return CommunicationHandler::Pool::DATA;
}

} // namespace messages
} // namespace communication
} // namespace veil


#endif // VEILHELPERS_MESSAGES_H
