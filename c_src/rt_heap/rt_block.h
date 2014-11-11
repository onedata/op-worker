/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#ifndef RT_BLOCK_H
#define RT_BLOCK_H

#include "nifpp.h"

#include <string>

namespace one {
namespace provider {

class rt_block
{
public:
    /**
     * rt_block constructor.
     * Constructs empty RTransfer block.
     */
    rt_block()
    : rt_block("", 0, 0, 0) {}

    /**
     * rt_block constructor.
     * Constructs RTransfer block.
     * @param file_id ID of file this block is a part of
     * @param offset block offset
     * @param size block size
     * @param priority block priority
     */
    rt_block(std::string file_id,
             ErlNifUInt64 offset,
             ErlNifUInt64 size,
             int priority)
    : rt_block(file_id,
               offset,
               size,
               priority,
               1) {}

    /**
     * rt_block constructor.
     * Constructs RTransfer block.
     * @param file_id ID of file this block is a part of
     * @param offset block offset
     * @param size block size
     * @param priority block priority
     * @param counter defines how many times block was pushed on the rt_heap
     */
    rt_block(std::string file_id,
             ErlNifUInt64 offset,
             ErlNifUInt64 size,
             int priority,
             int counter)
    : file_id_(std::move(file_id))
    , offset_(offset)
    , size_(size)
    , priority_(priority)
    , counter_(counter) {}

    /**
     * rt_block destructor.
     * Destructs RTransfer block.
     */
    ~rt_block() {}

    /// Getter for block's file ID
    const std::string& file_id() const { return file_id_; }

    /// Getter for block's offset
    ErlNifUInt64 offset() const { return offset_; }

    /// Getter for block's size
    ErlNifUInt64 size() const { return size_; }

    /// Getter for block's end
    ErlNifUInt64 end() const { return offset_ + size_ - 1; }

    /// Getter for block's priority
    int priority() const { return priority_; }

    /// Getter for block's addition counter
    int counter() const { return counter_; }

    /**
     * Modifies this block by merging other block
     * @param block to be merged
     * @return merged block
     */
    const rt_block& merge(const rt_block& block);

    bool operator<(const rt_block& block) const;

private:
    std::string file_id_;
    ErlNifUInt64 offset_;
    ErlNifUInt64 size_;
    int priority_;
    int counter_;
};

} // namespace provider
} // namespace one

#endif // RT_BLOCK_H