//
// ip/impl/address.ipp
// ~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2015 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef ASIO_IP_IMPL_ADDRESS_IPP
#define ASIO_IP_IMPL_ADDRESS_IPP

#if defined(_MSC_VER) && (_MSC_VER >= 1200)
# pragma once
#endif // defined(_MSC_VER) && (_MSC_VER >= 1200)

#include "asio/detail/config.hpp"
#include <typeinfo>
#include "asio/detail/throw_error.hpp"
#include "asio/detail/throw_exception.hpp"
#include "asio/error.hpp"
#include "asio/ip/address.hpp"
#include "asio/ip/bad_address_cast.hpp"
#include "asio/system_error.hpp"

#include "asio/detail/push_options.hpp"

namespace asio {
namespace ip {

address::address()
  : type_(none),
    ipv4_address_(),
    ipv6_address_()
{
}

address::address(const asio::ip::address_v4& ipv4_address)
  : type_(ipv4),
    ipv4_address_(ipv4_address),
    ipv6_address_()
{
}

address::address(const asio::ip::address_v6& ipv6_address)
  : type_(ipv6),
    ipv4_address_(),
    ipv6_address_(ipv6_address)
{
}

address::address(const address& other)
  : type_(other.type_),
    ipv4_address_(other.ipv4_address_),
    ipv6_address_(other.ipv6_address_)
{
}

#if defined(ASIO_HAS_MOVE)
address::address(address&& other)
  : type_(other.type_),
    ipv4_address_(other.ipv4_address_),
    ipv6_address_(other.ipv6_address_)
{
}
#endif // defined(ASIO_HAS_MOVE)

address& address::operator=(const address& other)
{
  type_ = other.type_;
  ipv4_address_ = other.ipv4_address_;
  ipv6_address_ = other.ipv6_address_;
  return *this;
}

#if defined(ASIO_HAS_MOVE)
address& address::operator=(address&& other)
{
  type_ = other.type_;
  ipv4_address_ = other.ipv4_address_;
  ipv6_address_ = other.ipv6_address_;
  return *this;
}
#endif // defined(ASIO_HAS_MOVE)

address& address::operator=(const asio::ip::address_v4& ipv4_address)
{
  type_ = ipv4;
  ipv4_address_ = ipv4_address;
  ipv6_address_ = asio::ip::address_v6();
  return *this;
}

address& address::operator=(const asio::ip::address_v6& ipv6_address)
{
  type_ = ipv6;
  ipv4_address_ = asio::ip::address_v4();
  ipv6_address_ = ipv6_address;
  return *this;
}

address make_address(const char* str)
{
  asio::error_code ec;
  address addr = make_address(str, ec);
  asio::detail::throw_error(ec);
  return addr;
}

address make_address(const char* str, asio::error_code& ec)
{
  asio::ip::address_v6 ipv6_address =
    asio::ip::make_address_v6(str, ec);
  if (!ec)
    return address(ipv6_address);

  asio::ip::address_v4 ipv4_address =
    asio::ip::make_address_v4(str, ec);
  if (!ec)
    return address(ipv4_address);

  return address();
}

address make_address(const std::string& str)
{
  return make_address(str.c_str());
}

address make_address(const std::string& str,
    asio::error_code& ec)
{
  return make_address(str.c_str(), ec);
}

#if !defined(ASIO_NO_DEPRECATED)
asio::ip::address_v4 address::to_v4() const
{
  if (type_ != ipv4)
  {
    bad_address_cast ex;
    asio::detail::throw_exception(ex);
  }
  return ipv4_address_;
}

asio::ip::address_v6 address::to_v6() const
{
  if (type_ != ipv6)
  {
    bad_address_cast ex;
    asio::detail::throw_exception(ex);
  }
  return ipv6_address_;
}
#endif // !defined(ASIO_NO_DEPRECATED)

std::string address::to_string() const
{
  switch (type_)
  {
  case ipv4:
    return ipv4_address_.to_string();
  case ipv6:
    return ipv6_address_.to_string();
  default:
    bad_address_cast ex;
    asio::detail::throw_exception(ex);
    return std::string();
  }
}

std::string address::to_string(asio::error_code& ec) const
{
  switch (type_)
  {
  case ipv4:
    return ipv4_address_.to_string(ec);
  case ipv6:
    return ipv6_address_.to_string(ec);
  default:
    ec = asio::error::invalid_argument;
    return std::string();
  }
}

bool address::is_loopback() const
{
  switch (type_)
  {
  case ipv4:
    return ipv4_address_.is_loopback();
  case ipv6:
    return ipv6_address_.is_loopback();
  default:
    return false;
  }
}

bool address::is_unspecified() const
{
  switch (type_)
  {
  case ipv4:
    return ipv4_address_.is_unspecified();
  case ipv6:
    return ipv6_address_.is_unspecified();
  default:
    return false;
  }
}

bool address::is_multicast() const
{
  switch (type_)
  {
  case ipv4:
    return ipv4_address_.is_multicast();
  case ipv6:
    return ipv6_address_.is_multicast();
  default:
    return false;
  }
}

bool operator==(const address& a1, const address& a2)
{
  if (a1.type_ != a2.type_)
    return false;
  if (a1.type_ == address::ipv4)
    return a1.ipv4_address_ == a2.ipv4_address_;
  if (a1.type_ == address::ipv6)
    return a1.ipv6_address_ == a2.ipv6_address_;
  return true;
}

bool operator<(const address& a1, const address& a2)
{
  if (a1.type_ < a2.type_)
    return true;
  if (a1.type_ > a2.type_)
    return false;
  if (a1.type_ == address::ipv4)
    return a1.ipv4_address_ < a2.ipv4_address_;
  if (a1.type_ == address::ipv6)
    return a1.ipv6_address_ < a2.ipv6_address_;
  return false;
}

} // namespace ip
} // namespace asio

#include "asio/detail/pop_options.hpp"

#endif // ASIO_IP_IMPL_ADDRESS_IPP
