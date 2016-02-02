/**
 * @file proxyIORequest.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "proxyIORequest.h"

#include "messages.pb.h"

namespace one {
namespace messages {
namespace proxyio {

ProxyIORequest::ProxyIORequest(std::string spaceId, std::string storageId, std::string fileId)
    : m_spaceId{std::move(spaceId)}
    , m_storageId{std::move(storageId)}
    , m_fileId{std::move(fileId)}
{
}

std::unique_ptr<ProtocolClientMessage> ProxyIORequest::serializeAndDestroy()
{
    auto msg = std::make_unique<ProtocolClientMessage>();
    auto proxyio = msg->mutable_proxyio_request();

    proxyio->mutable_space_id()->swap(m_spaceId);
    proxyio->mutable_storage_id()->swap(m_storageId);
    proxyio->mutable_file_id()->swap(m_fileId);

    return msg;
}

} // namespace proxyio
} // namespace messages
} // namespace one
