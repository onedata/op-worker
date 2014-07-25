/**
 * @file connection.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATION_CONNECTION_H
#define VEILHELPERS_COMMUNICATION_CONNECTION_H


#include <exception>
#include <functional>
#include <memory>

namespace veil
{
namespace communication
{

class Connection
{
public:
    Connection(std::function<void(const std::string&)> onMessageCallback,
               std::function<void(Connection&, std::exception_ptr)> onFailCallback,
               std::function<void(Connection&)> onOpenCallback,
               std::function<void(Connection&)> onErrorCallback);

    virtual ~Connection();

    virtual void send(const std::string &payload) = 0;

protected:
    std::function<void(const std::string&)> m_onMessageCallback;
    std::function<void(std::exception_ptr)> m_onFailCallback;
    std::function<void()> m_onOpenCallback;
    std::function<void()> m_onErrorCallback;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATION_CONNECTION_H
