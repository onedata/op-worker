/**
 * @file base64.cpp
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>

#include <algorithm>

namespace one {
namespace proxy {
namespace utils {

using bin_to_base64 = boost::archive::iterators::base64_from_binary<
    boost::archive::iterators::transform_width<
        std::vector<unsigned char>::const_iterator, 6, 8>>;

using base64_to_bin = boost::archive::iterators::transform_width<
    boost::archive::iterators::binary_from_base64<std::string::const_iterator>,
    8, 6>;

std::string base64_encode(const std::vector<unsigned char> &binary)
{
    auto last_block_size = binary.size() % 3;
    const std::string padding(last_block_size ? 3 - (last_block_size) : 0, '=');
    return std::string(bin_to_base64(binary.begin()),
                       bin_to_base64(binary.end())) +
           padding;
}

std::vector<unsigned char> base64_decode(std::string base)
{
    while (base.back() == '=')
        base.pop_back();

    return std::vector<unsigned char>(base64_to_bin(base.begin()),
                                      base64_to_bin(base.end()));
}

} // namespace utils
} // namespace proxy
} // namespace one
