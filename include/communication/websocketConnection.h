/**
 * @file websocketConnection.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_WEBSOCKET_CONNECTION_H
#define VEILHELPERS_WEBSOCKET_CONNECTION_H


#include "connection.h"

#include <memory>

namespace veil
{
namespace communication
{

class Mailbox;

class WebsocketConnection: public Connection
{
public:
    WebsocketConnection(std::shared_ptr<Mailbox> mailbox);
    void send() override;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_WEBSOCKET_CONNECTION_H
