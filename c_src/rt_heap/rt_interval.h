/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#ifndef RT_INTERVAL_H
#define RT_INTERVAL_H

#include "nifpp.h"

#include <stdexcept>

namespace one {
namespace provider {

class rt_interval
{
public:
    /**
     * rt_interval constructor.
     * Constructs RTransfer interval.
     * @param offset interval offset
     * @param size interval size
     */
    rt_interval(ErlNifUInt64 offset,
                ErlNifUInt64 size);

    /**
     * rt_interval destructor.
     * Destructs RTransfer interval.
     */
    ~rt_interval() {}

    /// Getter for interval beginning
    ErlNifUInt64 begin() const { return begin_; }

    /// Getter for interval ending
    ErlNifUInt64 end() const { return end_; }

    bool operator<(const rt_interval& interval) const;

private:
    ErlNifUInt64 begin_;
    ErlNifUInt64 end_;
};

} // namespace provider
} // namespace one

#endif // RT_INTERVAL_H