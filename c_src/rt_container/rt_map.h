/**
 * @file rt_map.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_MAP_H
#define RT_MAP_H

#include "nifpp.h"
#include "rt_block.h"
#include "rt_interval.h"
#include "rt_container.h"

#include <set>

namespace one {
namespace provider {

/**
 * The rt_map class.
 * rt_map object represents RTransfer map that allows to
 * push and fetch rt_blocks for given range
 */
class rt_map : public rt_container {
public:
    /**
     * @copydoc rt_container::rt_container
     */
    rt_map(ErlNifUInt64 block_size) : rt_container{block_size} {}

    virtual void push(const rt_block &block) override;

    virtual rt_block fetch() override;

    virtual const std::set<rt_block> &fetch(ErlNifUInt64 offset,
                                            ErlNifUInt64 size) override;

    virtual ErlNifUInt64 size() const override;

private:
};

} // namespace provider
} // namespace one

#endif // RT_MAP_H
