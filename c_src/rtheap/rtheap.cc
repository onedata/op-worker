/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#include "rtheap.h"
#include <iostream>

using namespace boost::icl;

extern one::provider::rt_heap heap;

namespace one {
namespace provider {

bool operator == (const rt_block& left, const rt_block& right)
{
    return left.file_id() == right.file_id() &&
           left.offset() == right.offset() &&
           left.size() == right.size() &&
           left.priority() == right.priority() &&
           left.counter() == right.counter();
}

rt_block& rt_block::operator += (const rt_block& block)
{
    if(block.offset() < offset_ && size_ < block.size())
    {
        heap.erase(*this);
        ++counter_;
        heap.insert(*this);
        return *this;
    }
    else
    {
        long int left_offset = std::min(offset_, block.offset());
        long int middle_offset = std::max(offset_, block.offset());
        long int right_offset = std::min(offset_ + size_, block.offset() + block.size());

        rt_block left_block(file_id_,
                            left_offset,
                            middle_offset - left_offset,
                            priority_,
                            counter_);

        rt_block right_block(file_id_,
                             right_offset,
                             std::max(offset_ + size_, block.offset() + block.size()) - right_offset,
                             priority_,
                             counter_);

        heap.erase(*this);
        if(left_block.size() > 0)
            heap.insert(left_block);
        if(right_block.size() > 0)
            heap.insert(right_block);

        offset_ = middle_offset;
        size_ = right_offset - middle_offset;
        priority_ = block.priority();
        counter_ += block.counter();
        heap.insert(*this);

        return *this;
    }
}

void rt_heap::push(const rt_block& block)
{
    int i;
    for(i = 0; i < block.size() / MAX_RT_BLOCK_SIZE; ++i)
        files_[block.file_id()] += std::make_pair(
                                   discrete_interval<int>::right_open(block.offset() + i * MAX_RT_BLOCK_SIZE,
                                                                      block.offset() + (i + 1) * MAX_RT_BLOCK_SIZE),
                                   rt_block(block.file_id(),
                                            block.offset() + i * MAX_RT_BLOCK_SIZE,
                                            MAX_RT_BLOCK_SIZE,
                                            block.priority()));

    long int last_chunk_size = block.size() % MAX_RT_BLOCK_SIZE;
    if(last_chunk_size > 0)
        files_[block.file_id()] += std::make_pair(
                                   discrete_interval<int>::right_open(block.offset() + i * MAX_RT_BLOCK_SIZE,
                                                                      block.offset() + (i + 1) * MAX_RT_BLOCK_SIZE),
                                   rt_block(block.file_id(),
                                            block.offset() + i * MAX_RT_BLOCK_SIZE,
                                            last_chunk_size,
                                            block.priority()));

    insert(block);
}

const rt_block &rt_heap::fetch()
{
    rt_block block = *blocks_.begin();
    blocks_.erase(blocks_.begin());
    auto file = files_.find(block.file_id());
    file->second.erase(discrete_interval<int>::right_open(block.offset(),
                                                          block.offset() + block.size()));
    if(file->second.empty())
        files_.erase(file);
    return std::move(block);
}

const std::set< rt_block, rt_block_compare > &rt_heap::fetch_all()
{
    std::set< rt_block, rt_block_compare > blocks = blocks_;
    blocks_.clear();
    files_.clear();
    return std::move(blocks);
}

void rt_heap::insert(const rt_block& block)
{
    for(auto b : blocks_)
        std::cout << "{" << b.file_id() << ", " << b.offset() << ", " << b.size() << ", " << b.priority() << "}" << std::endl;
    blocks_.insert(block);
}

void rt_heap::erase(const rt_block& block)
{
    blocks_.erase(blocks_.find(block));
}

} // namespace provider
} // namespace one