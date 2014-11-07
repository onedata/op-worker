/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#ifndef RTHEAP_H
#define RTHEAP_H

#include <string>
#include <set>
#include <iostream>
#include <boost/icl/split_interval_map.hpp>

namespace one {
namespace provider {

const long int MAX_RT_BLOCK_SIZE = 10;

class rt_block
{
public:
    rt_block()
    : rt_block("", 0, 0, 0) {}

    rt_block(std::string file_id,
                 long int offset,
                 long int size,
                 long int priority)
    : rt_block(file_id, offset, size, priority, 1) {}

    rt_block(std::string file_id,
             long int offset,
             long int size,
             long int priority,
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
    long int priority() const { return priority_; }
    int counter() const { return counter_; }

    rt_block& operator += (const rt_block& block);

private:
    std::string file_id_;
    long int offset_;
    long int size_;
    long int priority_;
    int counter_;
};

struct rt_block_compare
{
    bool operator() (const rt_block& left, const rt_block& right) const
    {
        if(left.priority() > right.priority()) return true;
        else if(left.priority() < right.priority()) return false;
        else if(left.counter() > right.counter()) return true;
        else if(left.counter() < right.counter()) return false;
        else if(left.file_id() > right.file_id()) return true;
        else if(left.file_id() < right.file_id()) return false;
        else return left.offset() > right.offset();
    }
};

class rt_heap
{
public:
    rt_heap() {}

    ~rt_heap() {}

    void push(const rt_block& block);
    const rt_block &fetch();
    const std::set< rt_block, rt_block_compare > &fetch_all();
    void insert(const rt_block& block);
    void erase(const rt_block& block);

private:
    std::set< rt_block, rt_block_compare > blocks_;
    std::map< std::string, boost::icl::split_interval_map< int, rt_block > > files_;
};

} // namespace provider
} // namespace one

#endif // RTHEAP_H