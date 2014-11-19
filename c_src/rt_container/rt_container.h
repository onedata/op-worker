/**
 * @file rt_container.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_CONTAINER_H
#define RT_CONTAINER_H

#include "nifpp.h"
#include "rt_block.h"

#include <set>

namespace one {
namespace provider {

/**
 * The rt_container class.
 * rt_container object represents RTransfer container that allows to push and
 * fetch rt_blocks
 */
class rt_container {
public:
    /**
     * rt_container constructor.
     * Constructs RTransfer container.
     * @param block_size maximal size of block stored in the rt_container
     */
    rt_container(ErlNifUInt64 block_size) : block_size_{block_size} {}

    virtual ~rt_container() = default;

    /// Getter for maximal block size
    ErlNifUInt64 block_size() const { return block_size_; }

    /**
     * Pushes block on the rt_container. If block size is bigger than maximal
     * RTransfer block size it is split.
     * @param block to be pushed
     */
    virtual void push(const rt_block &block) = 0;

    /**
     * Fetches block from the top of rt_container
     * @return fetched block
     */
    virtual rt_block fetch() = 0;

    /**
     * Fetches blocks that matches consistent segment
     * @param offset beginning of segment
     * @param size length of segment
     * @return fetched blocks
     */
    virtual const std::set<rt_block> &fetch(ErlNifUInt64 offset,
                                            ErlNifUInt64 size) = 0;

    /**
     * Returns container size
     * @return container size
     */
    virtual ErlNifUInt64 size() const = 0;

protected:
    ErlNifUInt64 block_size_;
};

} // namespace provider
} // namespace one

#endif // RT_CONTAINER_H