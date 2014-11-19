/**
 * @file rt_priority_queue.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_PRIORITY_QUEUE_H
#define RT_PRIORITY_QUEUE_H

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
 * The rt_priority_queue class.
 * rt_priority_queue object represents RTransfer priority queue that allows to
 * push and fetch
 * rt_blocks
 */
class rt_priority_queue : public rt_container {
public:
    /**
     * @copydoc rt_container::rt_container
     */
    rt_priority_queue(ErlNifUInt64 block_size) : rt_container{block_size} {}

    virtual void push(const rt_block &block) override;

    virtual rt_block fetch() override;

    virtual const std::set<rt_block> &fetch(ErlNifUInt64 offset,
                                            ErlNifUInt64 size) override;

    virtual ErlNifUInt64 size() const override;

private:
    std::map<std::string, std::map<rt_interval, std::set<rt_block>::iterator>>
        files_blocks_;
    std::set<rt_block> blocks_;

    /// Internal function used to push block on the queue after possible split
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

#endif // RT_PRIORITY_QUEUE_H
