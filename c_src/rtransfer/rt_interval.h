/**
 * @file rt_interval.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_INTERVAL_H
#define RT_INTERVAL_H

#include "../nifpp.h"

namespace one {
namespace provider {

/**
 * The rt_interval class.
 * rt_interval object represents bytes range of rt_block
 * - [offset, offset + size)
 */
class rt_interval {
public:
    /**
     * rt_interval constructor.
     * Constructs RTransfer interval.
     * @param offset interval offset
     * @param size interval size
     */
    rt_interval(ErlNifUInt64 offset, ErlNifUInt64 size);

    /// Getter for interval beginning
    ErlNifUInt64 begin() const { return begin_; }

    /// Getter for interval ending
    ErlNifUInt64 end() const { return end_; }

    /**
     * Compares this interval with other interval.
     * Interval which ends earlier comes first.
     * @param interval to be compared with
     * @return true if this interval comes before the other, otherwise false
     */
    bool operator<(const rt_interval &interval) const;

private:
    ErlNifUInt64 begin_;
    ErlNifUInt64 end_;
};

} // namespace provider
} // namespace one

#endif // RT_INTERVAL_H
