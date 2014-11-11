/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#include "rt_heap.h"

namespace one {
namespace provider {

void rt_heap::push(const rt_block& block)
{
    long int i;
    for(i = 0; i < block.size() / block_size_; ++i)
        push_block(rt_block(block.file_id(),
                            block.offset() + i * block_size_,
                            block_size_,
                            block.priority(),
                            block.counter()));

    long int last_block_size = block.size() % block_size_;
    if(last_block_size > 0)
        push_block(rt_block(block.file_id(),
                            block.offset() + i * block_size_,
                            last_block_size,
                            block.priority(),
                            block.counter()));
}

rt_block rt_heap::fetch()
{
    if(blocks_.empty())
        throw std::runtime_error("Empty heap");

    rt_block block = *(blocks_.begin());

    blocks_.erase(blocks_.begin());
    auto& file_blocks = files_blocks_[block.file_id()];
    file_blocks.erase(rt_interval(block.offset(), block.size()));

    while(!file_blocks.empty())
    {
        rt_block next_block = *(blocks_.begin());
        if(is_mergeable(block, next_block))
        {
            block.merge(next_block);
            blocks_.erase(blocks_.begin());
            file_blocks.erase(rt_interval(next_block.offset(), next_block.size()));
        }
        else
        {
            break;
        }
    }

    if(file_blocks.empty())
        files_blocks_.erase(block.file_id());

    return std::move(block);
}

void rt_heap::push_block(const rt_block& block)
{
    auto& file_blocks = files_blocks_[block.file_id()];
    auto it = file_blocks.lower_bound(rt_interval(block.offset(), 1));

    if(it == file_blocks.end())
    {
        insert(file_blocks, block);
    }
    else
    {
        long int offset = block.offset();

        if(it->second->offset() < offset)
        {
            insert(file_blocks, rt_block(block.file_id(),
                                         it->second->offset(),
                                         offset - it->second->offset(),
                                         it->second->priority(),
                                         it->second->counter()));
        }

        while(it != file_blocks.end() && it->second->offset() <= block.end())
        {
            if(offset < it->second->offset())
            {
                insert(file_blocks, rt_block(block.file_id(),
                                             offset,
                                             it->second->offset() - offset,
                                             block.priority(),
                                             block.counter()));
                offset = it->second->offset();
            }
            else
            {
                if(block.end() < it->second->end())
                {
                    rt_block b1(block.file_id(),
                                offset,
                                block.end() - offset + 1,
                                block.priority(),
                                it->second->counter() + block.counter());
                    rt_block b2(block.file_id(),
                               block.end() + 1,
                               it->second->end() - block.end(),
                               it->second->priority(),
                               it->second->counter());
                    offset = it->second->end() + 1;
                    it = erase(file_blocks, it);
                    insert(file_blocks, b1);
                    insert(file_blocks, b2);
                }
                else
                {
                    rt_block b(block.file_id(),
                               offset,
                               it->second->end() - offset + 1,
                               block.priority(),
                               it->second->counter() + block.counter());
                    offset = it->second->end() + 1;
                    it = erase(file_blocks, it);
                    insert(file_blocks, b);
                }
            }
        }

        if(offset < block.end())
        {
            insert(file_blocks, rt_block(block.file_id(),
                                         offset,
                                         block.end() - offset + 1,
                                         block.priority(),
                                         block.counter()));
        }
    }
}

bool rt_heap::is_mergeable(const rt_block& lhs, const rt_block& rhs)
{
    return lhs.file_id() == rhs.file_id() &&
           lhs.offset() + lhs.size() == rhs.offset() &&
           lhs.size() + rhs.size() <= block_size_;
}

void rt_heap::insert(std::map< rt_interval, std::set< rt_block >::iterator >& file_blocks,
                     const rt_block& block)
{
    auto it = blocks_.insert(block);
    file_blocks[rt_interval(block.offset(), block.size())] = it.first;
}

std::map< rt_interval, std::set< rt_block >::iterator >::iterator rt_heap::erase
    (std::map< rt_interval, std::set< rt_block >::iterator >& file_blocks,
     const std::map< rt_interval, std::set< rt_block >::iterator >::iterator& it)
{
    auto it2 = it;
    ++it2;
    blocks_.erase(it->second);
    file_blocks.erase(it);
    return it2;
}

} // namespace provider
} // namespace one