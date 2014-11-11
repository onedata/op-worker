/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#include "rt_block.h"

namespace one {
namespace provider {

const rt_block& rt_block::merge(const rt_block& block)
{
    size_ += block.size();
    return *this;
}

bool rt_block::operator<(const rt_block& block) const
{
    if(priority_ > block.priority()) return true;
    else if(priority_ < block.priority()) return false;
    else if(counter_ > block.counter()) return true;
    else if(counter_ < block.counter()) return false;
    else if(file_id_ > block.file_id()) return true;
    else if(file_id_ < block.file_id()) return false;
    else if(offset_ < block.offset()) return true;
    else if(offset_ > block.offset()) return false;
    else return size_ < block.size();
}

bool operator==(const rt_block& lhs, const rt_block& rhs)
{
    return lhs.file_id() == rhs.file_id() &&
           lhs.offset() == rhs.offset() &&
           lhs.size() == rhs.size() &&
           lhs.priority() == rhs.priority() &&
           lhs.counter() == rhs.counter();
}

} // namespace provider
} // namespace one