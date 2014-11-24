/**
 * @file rt_map.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "rt_map.h"
#include "rt_block.h"
#include "rt_exception.h"

namespace one {
namespace provider {

void rt_map::put(const rt_block &block)
{
    files_[block.file_id()]
        += std::make_pair(boost::icl::discrete_interval<ErlNifUInt64>(
                              block.offset(), block.offset() + block.size()),
                          block);
}

std::list<rt_block> rt_map::get(std::string file_id, ErlNifUInt64 offset,
                                ErlNifUInt64 size)
{
    std::list<rt_block> blocks;
    auto result = files_[file_id].equal_range(
        boost::icl::discrete_interval<ErlNifUInt64>(offset, offset + size));
    for (auto it = result.first; it != result.second; ++it) {
        const auto &interval = it->first;
        const auto &block = it->second;
        ErlNifUInt64 begin = std::max<ErlNifUInt64>(interval.lower(), offset);
        ErlNifUInt64 end
            = std::min<ErlNifUInt64>(interval.upper(), offset + size);
        blocks.push_back(rt_block(file_id, block.provider_ref(), begin,
                                  end - begin, block.priority(), block.terms(),
                                  block.counter()));
    }

    if (!blocks.empty()) {
        std::list<rt_block> merged_blocks;
        rt_block front_block = blocks.front();
        blocks.pop_front();
        for (const auto &block : blocks)
            if (front_block.is_mergeable(block)) {
                front_block += block;
            } else {
                merged_blocks.push_back(front_block);
                front_block = block;
            }
        merged_blocks.push_back(front_block);
        return merged_blocks;
    }

    return blocks;
}

void rt_map::remove(std::string file_id, ErlNifUInt64 offset, ErlNifUInt64 size)
{
    files_[file_id].erase(
        boost::icl::discrete_interval<ErlNifUInt64>(offset, offset + size));
}

} // namespace provider
} // namespace one