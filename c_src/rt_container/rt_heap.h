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
#include "rt_container.h"

#include <map>
#include <set>
#include <list>
#include <string>
#include <stdexcept>

namespace one {
namespace provider {

/**
 * The rt_heap class.
 * rt_heap object represents RTransfer heap that allows to push and pop
 * rt_blocks
 */
class rt_heap : public rt_container {
public:
    /**
     * @copydoc rt_container::rt_container
     */
    rt_heap(ErlNifUInt64 block_size) : rt_container(block_size) {}
    /**
     * @copydoc rt_container::push
     */
    virtual void push(const rt_block &block) override;

    /**
     * @copydoc rt_container::pop
     */
    virtual rt_block pop() override;

private:
    std::map<std::string, std::map<rt_interval, std::set<rt_block>::iterator>>
        files_blocks_;
    std::set<rt_block> blocks_;

    /// Internal function used to push block on the heap after possible split
    void do_push(const rt_block &block);

    void insert(
        std::map<rt_interval, std::set<rt_block>::iterator> &file_blocks,
        const rt_block &block);

    std::map<rt_interval, std::set<rt_block>::iterator>::iterator erase(
        std::map<rt_interval, std::set<rt_block>::iterator> &file_blocks,
        const std::map<rt_interval, std::set<rt_block>::iterator>::iterator &
            it);
};

} // namespace provider
} // namespace one

#endif // RT_HEAP_H
