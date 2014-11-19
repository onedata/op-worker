/**
 * @file rt_map.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_MAP_H
#define RT_MAP_H

#include "nifpp.h"
#include "rt_block.h"
#include "rt_interval.h"

#include <list>

namespace one {
namespace provider {

/**
 * The rt_map class.
 * rt_map object represents RTransfer map that allows to
 * push and fetch rt_blocks for given range
 */
class rt_map {
public:
    /**
     * rt_map constructor.
     * Constructs RTransfer map.
     * @param block_size maximal size of block stored in the rt_map
     */
    rt_map(ErlNifUInt64 block_size) : block_size_{block_size} {}

    void push(const rt_block &block);

    const std::list<rt_block> &fetch(std::string file_id, ErlNifUInt64 offset,
                                     ErlNifUInt64 size);

    void remove(std::string file_id, ErlNifUInt64 offset, ErlNifUInt64 size);

private:
    ErlNifUInt64 block_size_;
};

} // namespace provider
} // namespace one

#endif // RT_MAP_H
