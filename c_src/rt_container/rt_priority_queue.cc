/**
 * @file rt_priority_queue.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "rt_priority_queue.h"

namespace one {
namespace provider {

void rt_priority_queue::push(const rt_block &block)
{
    for (ErlNifUInt64 i = 0; i < block.size() / block_size_; ++i)
        do_push(rt_block(block.file_id(), block.provider_ref(),
                         block.offset() + i * block_size_, block_size_,
                         block.priority(), block.terms(), block.counter()));

    ErlNifUInt64 full_block_amount = block.size() / block_size_;
    ErlNifUInt64 last_block_size = block.size() % block_size_;
    if (last_block_size > 0)
        do_push(rt_block(block.file_id(), block.provider_ref(),
                         block.offset() + full_block_amount * block_size_,
                         last_block_size, block.priority(), block.terms(),
                         block.counter()));
}

rt_block rt_priority_queue::fetch()
{
    if (blocks_.empty())
        throw std::runtime_error("Empty container");

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

const std::set<rt_block> &rt_priority_queue::fetch(ErlNifUInt64 offset,
                                                   ErlNifUInt64 size)
{
    throw std::runtime_error("Unsupported operation");
}

ErlNifUInt64 rt_priority_queue::size() const { return blocks_.size(); }

void rt_priority_queue::do_push(const rt_block &block)
{
    auto &file_blocks = files_blocks_[block.file_id()];
    auto it = file_blocks.lower_bound(rt_interval(block.offset(), 1));

    if (it == file_blocks.end()) {
        insert(file_blocks, block);
    } else {
        ErlNifUInt64 offset = block.offset();

        if (it->second->offset() < offset) {
            insert(file_blocks,
                   rt_block(it->second->file_id(), it->second->provider_ref(),
                            it->second->offset(), offset - it->second->offset(),
                            it->second->priority(), it->second->terms(),
                            it->second->counter()));
        }

        while (it != file_blocks.end() && it->second->offset() <= block.end()) {
            if (offset < it->second->offset()) {
                insert(file_blocks,
                       rt_block(block.file_id(), block.provider_ref(), offset,
                                it->second->offset() - offset, block.priority(),
                                block.terms(), block.counter()));
                offset = it->second->offset();
            } else {
                if (block.end() < it->second->end()) {
                    rt_block b1(block.file_id(), block.provider_ref(), offset,
                                block.end() - offset + 1, block.priority(),
                                it->second->terms(),
                                it->second->counter() + block.counter());
                    b1.appendTerms(block.terms());
                    rt_block b2(it->second->file_id(),
                                it->second->provider_ref(), block.end() + 1,
                                it->second->end() - block.end(),
                                it->second->priority(), it->second->terms(),
                                it->second->counter());
                    offset = it->second->end() + 1;
                    it = erase(file_blocks, it);
                    insert(file_blocks, b1);
                    insert(file_blocks, b2);
                } else {
                    rt_block b(block.file_id(), block.provider_ref(), offset,
                               it->second->end() - offset + 1, block.priority(),
                               it->second->terms(),
                               it->second->counter() + block.counter());
                    b.appendTerms(block.terms());
                    offset = it->second->end() + 1;
                    it = erase(file_blocks, it);
                    insert(file_blocks, b);
                }
            }
        }

        if (offset <= block.end()) {
            insert(file_blocks,
                   rt_block(block.file_id(), block.provider_ref(), offset,
                            block.end() - offset + 1, block.priority(),
                            block.terms(), block.counter()));
        }
    }
}

void rt_priority_queue::insert(
    std::map<rt_interval, std::set<rt_block>::iterator> &file_blocks,
    const rt_block &block)
{
    auto result = blocks_.insert(block);
    file_blocks[rt_interval(block.offset(), block.size())] = result.first;
}

std::map<rt_interval, std::set<rt_block>::iterator>::iterator
rt_priority_queue::erase(
    std::map<rt_interval, std::set<rt_block>::iterator> &file_blocks,
    const std::map<rt_interval, std::set<rt_block>::iterator>::iterator &it)
{
    blocks_.erase(it->second);
    return file_blocks.erase(it);
}

} // namespace provider
} // namespace one