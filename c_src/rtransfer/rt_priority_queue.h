/**
 * @file rt_priority_queue.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_PRIORITY_QUEUE_H
#define RT_PRIORITY_QUEUE_H

#include "../nifpp.h"
#include "rt_block.h"
#include "rt_interval.h"

#include <set>
#include <map>
#include <list>
#include <string>
#include <unordered_map>

namespace one {
namespace provider {

/**
 * The rt_priority_queue class.
 * rt_priority_queue object represents RTransfer priority queue that allows to
 * push and pop rt_blocks
 */
class rt_priority_queue {
public:
    /**
     * rt_priority_queue constructor.
     * Constructs RTransfer priority queue.
     * @param block_size maximal size of block stored in the rt_priority_queue
     */
    rt_priority_queue(ErlNifUInt64 block_size);

    /**
     * Pushes block on the rt_priority_queue. If block size is bigger than
     * maximal
     * RTransfer block size it is split.
     * @param block to be pushed
     */
    void push(const rt_block &block);

    /**
     * Pops block from the top of rt_priority_queue
     * @return popped block
     */
    rt_block pop();

    /**
     * For blocks from range [offset, offset + size) updates theirs counters by
     * 'change'
     * @param offset beginning of range
     * @param size length of range
     * @param change value to be added to current blocks' counter value
     */
    void change_counter(const std::string &file_id, ErlNifUInt64 offset,
                        ErlNifUInt64 size, ErlNifSInt64 change);

    /**
     * Returns container size
     * @return container size
     */
    ErlNifUInt64 size() const;

private:
    ErlNifUInt64 block_size_;
    std::unordered_map<std::string,
                       std::map<rt_interval, std::set<rt_block>::iterator>>
        files_blocks_;
    std::set<rt_block> blocks_;

    /// Internal function used to push block on the queue after possible split
    void do_push(rt_block block);

    void
    insert(std::map<rt_interval, std::set<rt_block>::iterator> &file_blocks,
           rt_block block);

    std::map<rt_interval, std::set<rt_block>::iterator>::iterator
    erase(std::map<rt_interval, std::set<rt_block>::iterator> &file_blocks,
          const std::map<rt_interval, std::set<rt_block>::iterator>::iterator &
              it);
};

} // namespace provider
} // namespace one

#endif // RT_PRIORITY_QUEUE_H
