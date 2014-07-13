/**
 * @file mailbox.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_MAILBOX_H
#define VEILHELPERS_MAILBOX_H


#include <string>

namespace veil
{
namespace communication
{

class Mailbox
{
public:
    void onMessage(std::string payload);

    Mailbox(const Mailbox&) = delete;
    Mailbox &operator=(const Mailbox&) = delete;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_MAILBOX_H
