/**
 * @file rt_heap.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_HEAP_H
#define RT_HEAP_H

#include "nifpp.h"
#include "rt_block.h"
#include "rt_interval.h"

#include <map>
#include <set>
#include <string>
#include <stdexcept>

namespace one {
namespace provider {

/**
 * The rt_heap class.
 * rt_heap object represents RTransfer heap that allows to push and fetch rt_blocks
 */
class rt_heap
{
public:
    /**
     * rt_heap constructor.
     * Constructs RTransfer heap.
     * @param block_size maximal size of block stored on the rt_heap
     */
    rt_heap(ErlNifUInt64 block_size)
    : block_size_(block_size) {}

    /// Getter for maximal block size
    ErlNifUInt64 block_size() const { return block_size_; }

    /**
     * Pushes block on the rt_heap. If block size is bigger than maximal RTransfer block size it is split.
     * @param block to be pushed
     */
    void push(const rt_block& block);

    /**
     * Fetches block from the rt_heap
     * @return fetched block
     */
    rt_block fetch();

private:
    ErlNifUInt64 block_size_;
    std::map< std::string, std::map< rt_interval, std::set< rt_block >::iterator > > files_blocks_;
    std::set< rt_block > blocks_;

    /// Internal function used to push block on the heap after possible split
    void do_push(const rt_block& block);

    void insert(std::map< rt_interval, std::set< rt_block >::iterator >& file_blocks,
                const rt_block& block);

    std::map< rt_interval, std::set< rt_block >::iterator >::iterator
    erase(std::map< rt_interval, std::set< rt_block >::iterator >& file_blocks,
          const std::map< rt_interval, std::set< rt_block >::iterator >::iterator& it);
};

} // namespace provider
} // namespace one

#endif // RT_HEAP_H
