/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#ifndef RT_BLOCK_H
#define RT_BLOCK_H

#include <iostream>
#include <string>

namespace one {
namespace provider {

class rt_block
{
public:
    rt_block()
    : rt_block("", 0, 0, 0) {}

    rt_block(std::string file_id,
             long int offset,
             long int size,
             int priority)
    : rt_block(file_id,
               offset,
               size,
               priority,
               1) {}

    rt_block(std::string file_id,
             long int offset,
             long int size,
             int priority,
             int counter)
    : file_id_(std::move(file_id))
    , offset_(offset)
    , size_(size)
    , priority_(priority)
    , counter_(counter) {}

    ~rt_block() {}

    const std::string& file_id() const { return file_id_; }
    long int offset() const { return offset_; }
    long int size() const { return size_; }
    long int end() const { return offset_ + size_ - 1; }
    int priority() const { return priority_; }
    int counter() const { return counter_; }

    const rt_block& merge(const rt_block& block);

    bool operator<(const rt_block& block) const;

private:
    std::string file_id_;
    long int offset_;
    long int size_;
    int priority_;
    int counter_;
};

} // namespace provider
} // namespace one

#endif // RT_BLOCK_H