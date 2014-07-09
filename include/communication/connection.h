/**
 * @file connection.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_CONNECTION_H
#define VEILHELPERS_CONNECTION_H


#include <memory>

namespace veil
{
namespace communication
{

class Mailbox;

class Connection
{
public:
    Connection(std::shared_ptr<Mailbox> mailbox);
    virtual ~Connection() = default;

    virtual void send() = 0;

protected:
    const std::shared_ptr<Mailbox> m_mailbox;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CONNECTION_H
