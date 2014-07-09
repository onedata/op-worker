/**
 * @file connectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/connectionPool.h"

namespace veil
{
namespace communication
{

ConnectionPool::ConnectionPool(const unsigned int connectionsNumber,
                               std::shared_ptr<Mailbox> mailbox,
                               const std::string &uri)
    : m_connectionsNumber{connectionsNumber}
    , m_mailbox{std::move(mailbox)}
    , m_uri{uri}
{
}

} // namespace communication
} // namespace veil
