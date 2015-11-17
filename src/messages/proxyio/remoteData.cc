/**
 * @file remoteData.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "remoteData.h"

#include "messages.pb.h"
#include "messages/status.h"

#include <sstream>

namespace one {
namespace messages {
namespace proxyio {

RemoteData::RemoteData(std::unique_ptr<ProtocolServerMessage> serverMessage)
    : m_data{std::move(*serverMessage->mutable_remote_data()->mutable_data())}
{
    if (!serverMessage->has_remote_data())
        throw std::system_error{std::make_error_code(std::errc::protocol_error),
            "remote_data field missing"};

    auto &statusMsg = serverMessage->remote_data().status();
    Status(statusMsg).throwOnError();
}

std::string RemoteData::toString() const
{
    std::stringstream stream;
    stream << "type: 'RemoteData', data size: " << m_data.size();
    return stream.str();
}

} // namespace proxyio
} // namespace messages
} // namespace one
