/**
 * @file rt_priority_queue.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "rt_exception.h"
#include "rt_priority_queue.h"

#include <algorithm>

namespace one {
namespace provider {

rt_priority_queue::rt_priority_queue(ErlNifUInt64 block_size)
    : block_size_{block_size}
{
}

void rt_priority_queue::push(const rt_block &block)
{
    for (ErlNifUInt64 i = 0; i < block.size() / block_size_; ++i)
        do_push(rt_block(block.file_id(), block.provider_ref(),
                         block.offset() + i * block_size_, block_size_,
                         block.priority(), block.retry(), block.terms(),
                         block.counter()));

    ErlNifUInt64 full_block_amount = block.size() / block_size_;
    ErlNifUInt64 last_block_size = block.size() % block_size_;
    if (last_block_size > 0)
        do_push(rt_block(block.file_id(), block.provider_ref(),
                         block.offset() + full_block_amount * block_size_,
                         last_block_size, block.priority(), block.retry(),
                         block.terms(), block.counter()));
}

rt_block rt_priority_queue::pop()
{
    if (blocks_.empty())
        throw rt_exception("empty");

    rt_block block = std::move(*(blocks_.begin()));

    blocks_.erase(blocks_.begin());
    auto &file_blocks = files_blocks_[block.file_id()];
    file_blocks.erase(rt_interval(block.offset(), block.size()));

    while (!file_blocks.empty()) {
        rt_block next_block = *(blocks_.begin());
        if (block.is_mergeable(next_block, block_size_)) {
            block += next_block;
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

void rt_priority_queue::change_counter(const std::string &file_id,
                                       ErlNifUInt64 offset, ErlNifUInt64 size,
                                       ErlNifSInt64 change = 1)
{
    auto find_result = files_blocks_.find(file_id);
    if (find_result == files_blocks_.end())
        return;

    auto &file_blocks = find_result->second;
    auto it = file_blocks.lower_bound(rt_interval(offset, 1));

    if (it == file_blocks.end())
        return;

    ErlNifUInt64 end = offset + size - 1;

    if (it->second->offset() < offset)
        insert(file_blocks,
               rt_block(it->second->file_id(), it->second->provider_ref(),
                        it->second->offset(), offset - it->second->offset(),
                        it->second->priority(), it->second->retry(),
                        it->second->terms(), it->second->counter()));
    else
        offset = it->second->offset();

    while (it != file_blocks.end() && it->second->offset() <= end) {
        if (end < it->second->end()) {
            rt_block b1(
                it->second->file_id(), it->second->provider_ref(), offset,
                end + 1 - offset, it->second->priority(), it->second->retry(),
                it->second->terms(),
                std::max<ErlNifUInt64>(0, it->second->counter() + change));
            rt_block b2(it->second->file_id(), it->second->provider_ref(),
                        end + 1, it->second->end() - end,
                        it->second->priority(), it->second->retry(),
                        it->second->terms(), it->second->counter());
            it = erase(file_blocks, it);
            insert(file_blocks, b1);
            insert(file_blocks, b2);
        } else {
            rt_block b(
                it->second->file_id(), it->second->provider_ref(), offset,
                it->second->end() + 1 - offset, it->second->priority(),
                it->second->retry(), it->second->terms(),
                std::max<ErlNifUInt64>(0, it->second->counter() + change));
            it = erase(file_blocks, it);
            if (it != file_blocks.end())
                offset = it->second->offset();
            insert(file_blocks, b);
        }
    }
}

ErlNifUInt64 rt_priority_queue::size() const { return blocks_.size(); }

void rt_priority_queue::do_push(rt_block block)
{
    auto &file_blocks = files_blocks_[block.file_id()];
    auto it = file_blocks.lower_bound(rt_interval(block.offset(), 1));

    if (it == file_blocks.end()) {
        insert(file_blocks, std::move(block));
    } else {
        ErlNifUInt64 offset = block.offset();

        if (it->second->offset() < offset) {
            insert(file_blocks,
                   rt_block(it->second->file_id(), it->second->provider_ref(),
                            it->second->offset(), offset - it->second->offset(),
                            it->second->priority(), it->second->retry(),
                            it->second->terms(), it->second->counter()));
        }

        while (it != file_blocks.end() && it->second->offset() <= block.end()) {
            if (offset < it->second->offset()) {
                insert(file_blocks,
                       rt_block(block.file_id(), block.provider_ref(), offset,
                                it->second->offset() - offset, block.priority(),
                                block.retry(), block.terms(), block.counter()));
                offset = it->second->offset();
            } else {
                if (block.end() < it->second->end()) {
                    rt_block b1(
                        block.file_id(), block.provider_ref(), offset,
                        block.end() - offset + 1, block.priority(),
                        std::max<int>(it->second->retry(), block.retry()),
                        it->second->terms(),
                        it->second->counter() + block.counter());
                    b1.appendTerms(block.terms());
                    rt_block b2(it->second->file_id(),
                                it->second->provider_ref(), block.end() + 1,
                                it->second->end() - block.end(),
                                it->second->priority(), it->second->retry(),
                                it->second->terms(), it->second->counter());
                    offset = it->second->end() + 1;
                    it = erase(file_blocks, it);
                    insert(file_blocks, std::move(b1));
                    insert(file_blocks, std::move(b2));
                } else {
                    rt_block b(
                        block.file_id(), block.provider_ref(), offset,
                        it->second->end() - offset + 1, block.priority(),
                        std::max<int>(it->second->retry(), block.retry()),
                        it->second->terms(),
                        it->second->counter() + block.counter());
                    b.appendTerms(block.terms());
                    offset = it->second->end() + 1;
                    it = erase(file_blocks, it);
                    insert(file_blocks, std::move(b));
                }
            }
        }

        if (offset <= block.end()) {
            insert(file_blocks,
                   rt_block(block.file_id(), block.provider_ref(), offset,
                            block.end() - offset + 1, block.priority(),
                            block.retry(), block.terms(), block.counter()));
        }
    }
}

void rt_priority_queue::insert(
    std::map<rt_interval, std::set<rt_block>::iterator> &file_blocks,
    rt_block block)
{
    auto result = blocks_.emplace(std::move(block));
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
