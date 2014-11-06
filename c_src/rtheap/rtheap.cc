/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#include "rtheap.h"
#include <iostream>

namespace one {
namespace provider {

void rt_heap::push(const rt_block& block)
{
    std::cout << "Push file_id: " << block.file_id()
              << ", offset: " << block.offset()
              << ", size: " << block.size()
              << ", priority: " << block.priority() << std::endl;
}

} // namespace provider
} // namespace one