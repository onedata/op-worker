//
// executor_work.hpp
// ~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2015 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef ASIO_EXECUTOR_WORK_HPP
#define ASIO_EXECUTOR_WORK_HPP

#if defined(_MSC_VER) && (_MSC_VER >= 1200)
# pragma once
#endif // defined(_MSC_VER) && (_MSC_VER >= 1200)

#include "asio/detail/config.hpp"
#include "asio/associated_executor.hpp"
#include "asio/detail/type_traits.hpp"

#include "asio/detail/push_options.hpp"

namespace asio {

/// An object of type @c executor_work controls ownership of executor work
/// within a scope.
template <typename Executor>
class executor_work
{
public:
  /// The underlying executor type.
  typedef Executor executor_type;

  /// Constructs a @c executor_work object for the specified executor.
  /**
   * Stores a copy of @c e and calls <tt>on_work_started()</tt> on it.
   */
  explicit executor_work(const executor_type& e) ASIO_NOEXCEPT
    : executor_(e),
      owns_(true)
  {
    executor_.on_work_started();
  }

  /// Copy constructor.
  executor_work(const executor_work& other) ASIO_NOEXCEPT
    : executor_(other.executor_),
      owns_(other.owns_)
  {
    if (owns_)
      executor_.on_work_started();
  }

#if defined(ASIO_HAS_MOVE) || defined(GENERATING_DOCUMENTATION)
  /// Move constructor.
  executor_work(executor_work&& other)
    : executor_(ASIO_MOVE_CAST(Executor)(other.executor_)),
      owns_(other.owns_)
  {
    other.owns_ = false;
  }
#endif //  defined(ASIO_HAS_MOVE) || defined(GENERATING_DOCUMENTATION)

  /// Destructor.
  /**
   * Unless the object has already been reset, or is in a moved-from state,
   * calls <tt>on_work_finished()</tt> on the stored executor.
   */
  ~executor_work()
  {
    if (owns_)
      executor_.on_work_finished();
  }

  /// Obtain the associated executor.
  executor_type get_executor() const ASIO_NOEXCEPT
  {
    return executor_;
  }

  /// Whether the executor_work object owns some outstanding work.
  bool owns_work() const ASIO_NOEXCEPT
  {
    return owns_;
  }

  /// Indicate that the work is no longer outstanding.
  /*
   * Unless the object has already been reset, or is in a moved-from state,
   * calls <tt>on_work_finished()</tt> on the stored executor.
   */
  void reset() ASIO_NOEXCEPT
  {
    if (owns_)
    {
      executor_.on_work_finished();
      owns_ = false;
    }
  }

private:
  // Disallow assignment.
  executor_work& operator=(const executor_work&);

  executor_type executor_;
  bool owns_;
};

/// Create an @ref executor_work object.
template <typename Executor>
inline executor_work<Executor> make_work(const Executor& ex,
    typename enable_if<is_executor<Executor>::value>::type* = 0)
{
  return executor_work<Executor>(ex);
}

/// Create an @ref executor_work object.
template <typename ExecutionContext>
inline executor_work<typename ExecutionContext::executor_type>
make_work(ExecutionContext& ctx,
    typename enable_if<
      is_convertible<ExecutionContext&, execution_context&>::value>::type* = 0)
{
  return executor_work<typename ExecutionContext::executor_type>(
      ctx.get_executor());
}

/// Create an @ref executor_work object.
template <typename T>
inline executor_work<typename associated_executor<T>::type>
make_work(const T& t,
    typename enable_if<!is_executor<T>::value &&
      !is_convertible<T&, execution_context&>::value>::type* = 0)
{
  return executor_work<typename associated_executor<T>::type>(
      associated_executor<T>::get(t));
}

/// Create an @ref executor_work object.
template <typename T, typename Executor>
inline executor_work<typename associated_executor<T, Executor>::type>
make_work(const T& t, const Executor& ex,
    typename enable_if<is_executor<Executor>::value>::type* = 0)
{
  return executor_work<typename associated_executor<T, Executor>::type>(
      associated_executor<T, Executor>::get(t, ex));
}

/// Create an @ref executor_work object.
template <typename T, typename ExecutionContext>
inline executor_work<typename associated_executor<T,
  typename ExecutionContext::executor_type>::type>
make_work(const T& t, ExecutionContext& ctx,
    typename enable_if<!is_executor<T>::value &&
      !is_convertible<T&, execution_context&>::value>::type* = 0)
{
  return executor_work<typename associated_executor<T,
    typename ExecutionContext::executor_type>::type>(
      associated_executor<T, typename ExecutionContext::executor_type>::get(
        t, ctx.get_executor()));
}

} // namespace asio

#include "asio/detail/pop_options.hpp"

#endif // ASIO_EXECUTOR_WORK_HPP
