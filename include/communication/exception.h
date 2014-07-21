/**
 * @file exception.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATION_EXCEPTION_H
#define VEILHELPERS_COMMUNICATION_EXCEPTION_H


#include <exception>

namespace veil
{
namespace communication
{

class Exception: public std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};

class SendError: public Exception
{
public:
    using Exception::Exception;
};

class ConnectionError: public Exception
{
public:
    using Exception::Exception;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATION_EXCEPTION_H
