/**
 * @file rt_heap.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "rt_heap.h"

namespace one {
namespace provider {

void rt_heap::push(const rt_block &block)
{
    for (ErlNifUInt64 i = 0; i < block.size() / block_size_; ++i)
        do_push(rt_block(block.file_id(), block.offset() + i * block_size_,
                         block_size_, block.priority(), block.pids(),
                         block.counter()));

    ErlNifUInt64 full_block_amount = block.size() / block_size_;
    ErlNifUInt64 last_block_size = block.size() % block_size_;
    if (last_block_size > 0)
        do_push(rt_block(
            block.file_id(), block.offset() + full_block_amount * block_size_,
            last_block_size, block.priority(), block.pids(), block.counter()));
}

rt_block rt_heap::fetch()
{
    if (blocks_.empty())
        throw std::runtime_error("Empty heap");

    rt_block block = std::move(*(blocks_.begin()));

    blocks_.erase(blocks_.begin());
    auto &file_blocks = files_blocks_[block.file_id()];
    file_blocks.erase(rt_interval(block.offset(), block.size()));

    while (!file_blocks.empty()) {
        rt_block next_block = *(blocks_.begin());
        if (block.is_mergeable(next_block, block_size_)) {
            block.merge(next_block);
            blocks_.erase(blocks_.begin());
            file_blocks.erase(
                rt_interval(next_block.offset(), next_block.size()));
        } else {
            break;
        }
    }

    if (file_blocks.empty())
        files_blocks_.erase(block.file_id());

    return block;
}

void rt_heap::do_push(const rt_block &block)
{
    auto &file_blocks = files_blocks_[block.file_id()];
    auto it = file_blocks.lower_bound(rt_interval(block.offset(), 1));

    if (it == file_blocks.end()) {
        insert(file_blocks, block);
    } else {
        ErlNifUInt64 offset = block.offset();

        if (it->second->offset() < offset) {
            insert(file_blocks,
                   rt_block(block.file_id(), it->second->offset(),
                            offset - it->second->offset(),
                            it->second->priority(), it->second->pids(),
                            it->second->counter()));
        }

        while (it != file_blocks.end() && it->second->offset() <= block.end()) {
            if (offset < it->second->offset()) {
                insert(file_blocks,
                       rt_block(block.file_id(), offset,
                                it->second->offset() - offset, block.priority(),
                                block.pids(), block.counter()));
                offset = it->second->offset();
            } else {
                if (block.end() < it->second->end()) {
                    rt_block b1(block.file_id(), offset,
                                block.end() - offset + 1, block.priority(),
                                it->second->pids(),
                                it->second->counter() + block.counter());
                    b1.appendPids(block.pids());
                    rt_block b2(block.file_id(), block.end() + 1,
                                it->second->end() - block.end(),
                                it->second->priority(), it->second->pids(),
                                it->second->counter());
                    offset = it->second->end() + 1;
                    it = erase(file_blocks, it);
                    insert(file_blocks, b1);
                    insert(file_blocks, b2);
                } else {
                    rt_block b(block.file_id(), offset,
                               it->second->end() - offset + 1, block.priority(),
                               it->second->pids(),
                               it->second->counter() + block.counter());
                    b.appendPids(block.pids());
                    offset = it->second->end() + 1;
                    it = erase(file_blocks, it);
                    insert(file_blocks, b);
                }
            }
        }

        if (offset <= block.end()) {
            insert(file_blocks,
                   rt_block(block.file_id(), offset, block.end() - offset + 1,
                            block.priority(), block.pids(), block.counter()));
        }
    }
}

void rt_heap::insert(
    std::map<rt_interval, std::set<rt_block>::iterator> &file_blocks,
    const rt_block &block)
{
    auto result = blocks_.insert(block);
    file_blocks[rt_interval(block.offset(), block.size())] = result.first;
}

std::map<rt_interval, std::set<rt_block>::iterator>::iterator rt_heap::erase(
    std::map<rt_interval, std::set<rt_block>::iterator> &file_blocks,
    const std::map<rt_interval, std::set<rt_block>::iterator>::iterator &it)
{
    blocks_.erase(it->second);
    return file_blocks.erase(it);
}

} // namespace provider
} // namespace one