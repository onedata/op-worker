/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#ifndef RT_INTERVAL_H
#define RT_INTERVAL_H

#include <iostream>
#include <stdexcept>

namespace one {
namespace provider {

class rt_interval
{
public:
    rt_interval(long int offset,
                long int size);

    ~rt_interval() {}

    long int begin() const { return begin_; }
    long int end() const { return end_; }

    bool operator<(const rt_interval& interval) const;

private:
    long int begin_;
    long int end_;
};

} // namespace provider
} // namespace one

#endif // RT_INTERVAL_H