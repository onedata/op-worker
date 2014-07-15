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

class Connection
{
public:
    Connection(std::function<void(const std::string&)> onMessageCallback,
               std::function<void(Connection&)> onFailCallback,
               std::function<void(Connection&)> onOpenCallback,
               std::function<void(Connection&)> onErrorCallback);

    virtual ~Connection();

    virtual void send(const std::string &payload) = 0;

protected:
    virtual void close();

    std::function<void(const std::string&)> m_onMessageCallback;
    std::function<void()> m_onFailCallback;
    std::function<void()> m_onOpenCallback;
    std::function<void()> m_onErrorCallback;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CONNECTION_H
