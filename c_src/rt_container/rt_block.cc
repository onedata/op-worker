/**
 * @file rt_block.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "rt_block.h"

#include <algorithm>

namespace one {
namespace provider {

const rt_block &rt_block::merge(const rt_block &block)
{
    size_ += block.size_;
    for (const auto &term : block.terms_)
        terms_.push_back(term);
    return *this;
}

bool rt_block::is_mergeable(const rt_block &block, ErlNifUInt64 block_size)
{
    return file_id_ == block.file_id_ && offset_ + size_ == block.offset_
           && provider_ref_ == block.provider_ref_
           && size_ + block.size_ <= block_size;
}

void rt_block::appendTerms(const std::list<nifpp::TERM> &terms)
{
    terms_.insert(terms_.begin(), terms.begin(), terms.end());
}

bool rt_block::operator<(const rt_block &block) const
{
    if (priority_ > block.priority_)
        return true;
    if (priority_ < block.priority_)
        return false;
    if (counter_ > block.counter_)
        return true;
    if (counter_ < block.counter_)
        return false;
    if (file_id_ < block.file_id_)
        return true;
    if (file_id_ > block.file_id_)
        return false;
    if (offset_ < block.offset_)
        return true;
    if (offset_ > block.offset_)
        return false;
    return size_ < block.size_;
}

rt_block &rt_block::operator+=(const rt_block &block)
{
    provider_ref_ = block.provider_ref_;
    priority_ = std::max<ErlNifUInt64>(priority_, block.priority_);
    offset_ = std::max<ErlNifUInt64>(offset_, block.offset_);
    size_ = std::min<ErlNifUInt64>(offset_ + size_, block.offset_ + block.size_)
            - offset_;
    for (const auto &term : block.terms_)
        terms_.push_back(term);
    counter_ = std::max<ErlNifUInt64>(counter_, block.counter_);

    return *this;
}

bool operator==(const rt_block &lhs, const rt_block &rhs)
{
    return lhs.file_id() == rhs.file_id()
           && lhs.provider_ref() == rhs.provider_ref()
           && lhs.offset() == rhs.offset() && lhs.size() == rhs.size()
           && lhs.priority() == rhs.priority() && lhs.counter() == rhs.counter()
           && lhs.terms() == rhs.terms();
}

} // namespace provider
} // namespace one