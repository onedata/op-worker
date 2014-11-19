/**
 * @file rt_priority_queue.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "rt_map.h"
#include "rt_exception.h"

namespace one {
namespace provider {

void rt_map::push(const rt_block &block)
{
    throw rt_exception("unsupported_operation");
}

const std::set<rt_block> &rt_map::fetch(ErlNifUInt64 offset, ErlNifUInt64 size)
{
    throw rt_exception("unsupported_operation");
}

void &rt_map::remove(ErlNifUInt64 offset, ErlNifUInt64 size)
{
    throw rt_exception("unsupported_operation");
}

} // namespace provider
} // namespace one