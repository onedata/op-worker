//
// async_result.hpp
// ~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2015 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef ARCHETYPES_ASYNC_RESULT_HPP
#define ARCHETYPES_ASYNC_RESULT_HPP

#include <asio/async_result.hpp>
#include <asio/handler_type.hpp>

namespace archetypes {

struct lazy_handler
{
};

struct concrete_handler
{
  concrete_handler(lazy_handler)
  {
  }

  template <typename Arg1>
  void operator()(Arg1)
  {
  }

  template <typename Arg1, typename Arg2>
  void operator()(Arg1, Arg2)
  {
  }

#if defined(ASIO_HAS_MOVE)
  concrete_handler(concrete_handler&&) {}
private:
  concrete_handler(const concrete_handler&);
#endif // defined(ASIO_HAS_MOVE)
};

} // namespace archetypes

namespace asio {

template <typename Signature>
struct handler_type<archetypes::lazy_handler, Signature>
{
  typedef archetypes::concrete_handler type;
};

template <>
class async_result<archetypes::concrete_handler>
{
public:
  // The return type of the initiating function.
  typedef int type;

  // Construct an async_result from a given handler.
  explicit async_result(archetypes::concrete_handler&)
  {
  }

  // Obtain the value to be returned from the initiating function.
  type get()
  {
    return 42;
  }
};

} // namespace asio

#endif // ARCHETYPES_ASYNC_RESULT_HPP
