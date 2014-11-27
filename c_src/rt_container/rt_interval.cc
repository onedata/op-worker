/**
 * @file rt_interval.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "rt_interval.h"
#include "rt_exception.h"

namespace one {
namespace provider {

rt_interval::rt_interval(ErlNifUInt64 offset, ErlNifUInt64 size)
    : begin_(offset)
    , end_(offset + size - 1)
{
    if (size == 0)
        throw rt_exception("invalid_interval");
}

bool rt_interval::operator<(const rt_interval &interval) const
{
    return end_ < interval.end();
}

} // namespace provider
} // namespace one
