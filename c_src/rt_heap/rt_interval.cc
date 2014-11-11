/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#include "rt_interval.h"

namespace one {
namespace provider {

rt_interval::rt_interval(ErlNifUInt64 offset,
                 ErlNifUInt64 size)
    : begin_(offset)
    , end_(offset + size - 1)
{
    if(offset < 0 || size <= 0)
        throw std::runtime_error("Invalid interval");
}

bool rt_interval::operator<(const rt_interval& interval) const
{
    return end_ < interval.end();
}

bool operator==(const rt_interval& lhs, const rt_interval& rhs)
{
    return lhs.begin() == rhs.begin() &&
           lhs.end() == rhs.end();
}

} // namespace provider
} // namespace one