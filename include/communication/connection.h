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

class Connection // TODO: knowing when the connection is closed and when it's open
{
public:
    Connection(std::shared_ptr<Mailbox> mailbox,
               std::function<void(Connection&)> onFailCallback,
               std::function<void(Connection&)> onOpenCallback,
               std::function<void(Connection&)> onErrorCallback);

    virtual ~Connection();
    Connection(const Connection&) = delete;
    Connection &operator=(const Connection&) = delete;

    virtual void send(const std::string &payload) = 0;

protected:
    virtual void close();

    const std::shared_ptr<Mailbox> m_mailbox;
    std::function<void()> m_onFailCallback;
    std::function<void()> m_onOpenCallback;
    std::function<void()> m_onErrorCallback;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CONNECTION_H
