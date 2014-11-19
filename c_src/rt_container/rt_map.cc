/**
 * @file rt_priority_queue.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "rt_map.h"

namespace one {
namespace provider {

void rt_map::push(const rt_block &block)
{
    throw std::runtime_error("Unsupported operation");
}

rt_block rt_map::fetch() { throw std::runtime_error("Unsupported operation"); }

const std::set<rt_block> &rt_map::fetch(ErlNifUInt64 offset, ErlNifUInt64 size)
{
    throw std::runtime_error("Unsupported operation");
}

ErlNifUInt64 rt_map::size() const
{
    throw std::runtime_error("Unsupported operation");
}

} // namespace provider
} // namespace one