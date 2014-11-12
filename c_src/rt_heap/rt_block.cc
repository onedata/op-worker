/**
 * @file rt_block.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "rt_block.h"

namespace one {
namespace provider {

const rt_block& rt_block::merge(const rt_block& block)
{
    size_ += block.size();
    return *this;
}

bool rt_block::is_mergeable(const rt_block& block, ErlNifUInt64 block_size)
{
    return file_id_ == block.file_id() &&
           offset_ + size_ == block.offset() &&
           size_ + block.size() <= block_size;
}

bool rt_block::operator<(const rt_block& block) const
{
    if(priority_ > block.priority()) return true;
    if(priority_ < block.priority()) return false;
    if(counter_ > block.counter()) return true;
    if(counter_ < block.counter()) return false;
    if(file_id_ < block.file_id()) return true;
    if(file_id_ > block.file_id()) return false;
    if(offset_ < block.offset()) return true;
    if(offset_ > block.offset()) return false;
    return size_ < block.size();
}

} // namespace provider
} // namespace one