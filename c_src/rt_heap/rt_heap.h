/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

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

    /**
     * rt_block destructor.
     * Destructs RTransfer heap.
     */
    ~rt_heap() {}

    /// Getter for maximal block size
    ErlNifUInt64 block_size() const { return block_size_; }

    /**
     * Pushes block on the rt_heap
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

    void push_block(const rt_block& block);

    bool is_mergeable(const rt_block& lhs, const rt_block& rhs);

    void insert(std::map< rt_interval, std::set< rt_block >::iterator >& file_blocks,
                const rt_block& block);

    std::map< rt_interval, std::set< rt_block >::iterator >::iterator
    erase(std::map< rt_interval, std::set< rt_block >::iterator >& file_blocks,
          const std::map< rt_interval, std::set< rt_block >::iterator >::iterator& it);
};

} // namespace provider
} // namespace one

#endif // RT_HEAP_H
