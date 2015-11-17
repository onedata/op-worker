/**
 * @file remoteWrite.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_MESSAGES_PROXYIO_REMOTE_WRITE_H
#define HELPERS_MESSAGES_PROXYIO_REMOTE_WRITE_H

#include "messages/clientMessage.h"

#include <asio/buffer.hpp>

#include <sys/types.h>

#include <cstdint>
#include <string>

namespace one {
namespace messages {

class RemoteWrite : public ClientMessage {
public:
    RemoteWrite(std::string storageId, std::string fileId, const off_t offset,
        asio::const_buffer data);

    std::string toString() const override;

private:
    std::unique_ptr<ProtocolClientMessage> serializeAndDestroy() override;

    std::string m_storageId;
    std::string m_fileId;
    const off_t m_offset;
    asio::const_buffer m_data;
};

} // namespace messages
} // namespace one

#endif // HELPERS_MESSAGES_PROXYIO_REMOTE_WRITE_H
