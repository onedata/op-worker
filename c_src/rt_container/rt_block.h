/**
 * @file rt_block.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_BLOCK_H
#define RT_BLOCK_H

#include "nifpp.h"

#include <list>
#include <string>

namespace one {
namespace provider {

/**
 * The rt_block class.
 * rt_block object represents single block pushed on RTransfer heap
 */
class rt_block {
public:
    /**
     * rt_block constructor.
     * Constructs empty RTransfer block.
     */
    rt_block() : rt_block("", 0, 0, 0, std::list<ErlNifPid>()) {}

    /**
     * rt_block constructor.
     * Constructs RTransfer block.
     * @param file_id ID of file this block is a part of
     * @param offset block offset
     * @param size block size
     * @param priority block priority
     * @param counter defines how many times block was pushed on the rt_heap
     */
    rt_block(std::string file_id, ErlNifUInt64 offset, ErlNifUInt64 size,
             int priority, std::list<ErlNifPid> pids, int counter = 1)
        : file_id_{std::move(file_id)}
        , offset_{offset}
        , size_{size}
        , priority_{priority}
        , pids_{std::move(pids)}
        , counter_{counter}
    {
    }

    /// Getter for block's file ID
    const std::string &file_id() const { return file_id_; }

    /// Getter for block's offset
    ErlNifUInt64 offset() const { return offset_; }

    /// Getter for block's size
    ErlNifUInt64 size() const { return size_; }

    /// Getter for block's end
    ErlNifUInt64 end() const { return offset_ + size_ - 1; }

    /// Getter for block's priority
    int priority() const { return priority_; }

    /// Getter for block's pids
    const std::list<ErlNifPid> &pids() const { return pids_; }

    /// Getter for block's addition counter
    int counter() const { return counter_; }

    /**
     * Appends list of pids to block
     * @param list of pids to be appended to the list of block's pids
     */
    void appendPids(const std::list<ErlNifPid> &pids);

    /**
     * Modifies this block by merging other block
     * @param block to be merged
     * @return merged block
     */
    const rt_block &merge(const rt_block &block);

    /**
     * Checks whether this block can be merge with other block. That is
     * both belong to the same file, are successive and summary size is
     * less than maximal RTransfer block size.
     * @param block to be merged
     * @return merged block
     */
    bool is_mergeable(const rt_block &block, ErlNifUInt64 block_size);

    /**
     * Compares this block with other block. Order of comparison criteria:
     * 1) priority - block with higher priority comes first
     * 2) counter - block with higher addition count comes first
     * 3) file_id - block with lexicographically less file ID comes first
     * 4) offset - block with smaller offset comes first
     * 5) size - block with smaller size comes first
     * @param block to be compared with
     * @return true if this block comes before the other, otherwise false
     */
    bool operator<(const rt_block &block) const;

private:
    std::string file_id_;
    ErlNifUInt64 offset_;
    ErlNifUInt64 size_;
    int priority_;
    std::list<ErlNifPid> pids_;
    int counter_;
};

} // namespace provider
} // namespace one

#endif // RT_BLOCK_H