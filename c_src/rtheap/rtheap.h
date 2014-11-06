/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#ifndef RTHEAP_H
#define RTHEAP_H

#include <string>
#include <deque>

namespace one {
namespace provider {

class rt_block
{
public:
    rt_block(std::string file_id,
             long int offset,
             long int size,
             long int priority)
    : file_id_(std::move(file_id))
    , offset_(offset)
    , size_(size)
    , priority_(priority)
    , count_(1) {};

    ~rt_block();

    std::string file_id() const { return std::move(file_id_); }
    long int offset() const { return offset_; }
    long int size() const { return size_; }
    long int priority() const { return priority_; }
    int count() const { return count_; }

    rt_block& operator += (const rt_block& block)
    { return *this; }

private:
    std::string file_id_;
    long int offset_;
    long int size_;
    long int priority_;
    int count_;
};

class rt_heap
{
public:
    rt_heap();

    ~rt_heap();

    void push(const rt_block& block);

private:
    std::deque<rt_block> blocks_;
};

} // namespace provider
} // namespace one

#endif // RTHEAP_H