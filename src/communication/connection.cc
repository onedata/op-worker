/**
 * @file connection.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/connection.h"

namespace veil
{
namespace communication
{

Connection::Connection(std::shared_ptr<Mailbox> mailbox,
                       std::function<void(std::shared_ptr<Connection>)> onFailCallback,
                       std::function<void(std::shared_ptr<Connection>)> onOpenCallback,
                       std::function<void(std::shared_ptr<Connection>)> onErrorCallback)
    : m_mailbox{std::move(mailbox)}
{
    m_onFailCallback = [=]{ onFailCallback(shared_from_this()); };
    m_onOpenCallback = [=]{ onOpenCallback(shared_from_this()); };
    m_onErrorCallback = [=]{ onErrorCallback(shared_from_this()); };
}

void Connection::close()
{
    m_onFailCallback = m_onOpenCallback = m_onErrorCallback = []{};
}

} // namespace communication
} // namespace veil
