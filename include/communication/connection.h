/**
 * @file connection.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_CONNECTION_H
#define VEILHELPERS_CONNECTION_H


#include <functional>
#include <memory>

namespace veil
{
namespace communication
{

class Mailbox;

class Connection: public std::enable_shared_from_this<Connection> // TODO: knowing when the connection is closed and when it's open
{
public:
    Connection(std::shared_ptr<Mailbox> mailbox,
               std::function<void(std::shared_ptr<Connection>)> onFailCallback,
               std::function<void(std::shared_ptr<Connection>)> onOpenCallback,
               std::function<void(std::shared_ptr<Connection>)> onErrorCallback);

    virtual ~Connection() = default;

    virtual void send() = 0;
    virtual void close();

protected:
    const std::shared_ptr<Mailbox> m_mailbox;
    std::function<void()> m_onFailCallback;
    std::function<void()> m_onOpenCallback;
    std::function<void()> m_onErrorCallback;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CONNECTION_H
