/**
 * @file remoteWrite.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "remoteWrite.h"

#include "messages.pb.h"

#include <sstream>

namespace one {
namespace messages {

RemoteWrite::RemoteWrite(std::string storageId, std::string fileId,
    const off_t offset, asio::const_buffer data)
    : m_storageId{std::move(storageId)}
    , m_fileId{std::move(fileId)}
    , m_offset{offset}
    , m_data{data}
{
}

std::string RemoteWrite::toString() const
{
    std::stringstream stream;
    stream << "type: 'RemoteWrite', storageId: '" << m_storageId
           << "', fileId: '" << m_fileId << "', offset: " << m_offset
           << ", data size: " << asio::buffer_size(m_data);
    return stream.str();
}

std::unique_ptr<ProtocolClientMessage> RemoteWrite::serializeAndDestroy()
{
    auto clientMsg = std::make_unique<ProtocolClientMessage>();
    auto writeMsg = clientMsg->mutable_remote_write();

    writeMsg->mutable_storage_id()->swap(m_storageId);
    writeMsg->mutable_file_id()->swap(m_fileId);
    writeMsg->set_offset(m_offset);

    writeMsg->mutable_data()->assign(std::string(
        asio::buffer_cast<const char *>(m_data), asio::buffer_size(m_data)));

    return clientMsg;
}

} // namespace messages
} // namespace one
