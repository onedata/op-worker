/**
 * @file rt_map.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_MAP_H
#define RT_MAP_H

#include "../nifpp.h"
#include "rt_block.h"

#include <list>
#include <unordered_map>

#include <boost/icl/interval_map.hpp>

namespace one {
namespace provider {

/**
 * The rt_map class.
 * rt_map object represents RTransfer map that allows to
 * put and get rt_blocks for given range
 */
class rt_map {
public:
    /**
     * Extends the map by inserting new RTransfer block.
     * @param block to be put.
     */
    void put(const rt_block &block);

    /**
     * Returns list of RTransfer blocks for range given as [offset, offset +
     * size)
     * @param file_id ID of file blocks are a part of
     * @param offset range offset
     * @param size range size
     * @return list of blocks that belongs to the range
     */
    std::list<rt_block> get(const std::string &file_id, ErlNifUInt64 offset,
                            ErlNifUInt64 size) const;

    /**
     * Removes all RTransfer blocks for range given as [offset, offset + size)
     * @param file_id ID of file blocks are a part of
     * @param offset range offset
     * @param size range size
     */
    void remove(const std::string &file_id, ErlNifUInt64 offset,
                ErlNifUInt64 size);

private:
    std::unordered_map<std::string,
                       boost::icl::interval_map<ErlNifUInt64, rt_block>> files_;
};

} // namespace provider
} // namespace one

#endif // RT_MAP_H
