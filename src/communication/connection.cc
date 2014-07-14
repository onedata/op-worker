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

Connection::Connection(std::function<void(const std::string&)> onMessageCallback,
                       std::function<void(Connection&)> onFailCallback,
                       std::function<void(Connection&)> onOpenCallback,
                       std::function<void(Connection&)> onErrorCallback)
    : m_onMessageCallback{std::move(onMessageCallback)}
{
    m_onFailCallback = [=]{ onFailCallback(*this); };
    m_onOpenCallback = [=]{ onOpenCallback(*this); };
    m_onErrorCallback = [=]{ onErrorCallback(*this); };
}

Connection::~Connection()
{
    close();
}

void Connection::close()
{
    m_onMessageCallback = [](const std::string&){};
    m_onFailCallback = m_onOpenCallback = m_onErrorCallback = []{};
}

} // namespace communication
} // namespace veil
