/**
 * @file websocketConnection.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/websocketConnection.h"

namespace veil
{
namespace communication
{

WebsocketConnection::WebsocketConnection(std::shared_ptr<Mailbox> mailbox)
    : Connection{std::move(mailbox)}
{
}

void WebsocketConnection::send()
{
}

} // namespace communication
} // namespace veil
