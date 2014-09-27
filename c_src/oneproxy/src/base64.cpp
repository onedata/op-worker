#include <string>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>

namespace one {
namespace proxy {
namespace utils {

typedef boost::archive::iterators::base64_from_binary
    <boost::archive::iterators::transform_width
     <std::string::const_iterator, 6, 8>> bin_to_base64;

typedef boost::archive::iterators::transform_width
    <boost::archive::iterators::binary_from_base64
     <std::string::const_iterator>,
     8, 6> base64_to_bin;


std::string base64_encode(const std::string &binary)
{
    auto last_block_size = binary.size() % 3;
    const std::string padding(last_block_size ? 3 - (last_block_size) : 0, '=');
    return std::string(bin_to_base64(binary.begin()),
                       bin_to_base64(binary.end())) + padding;
}

std::string base64_decode(const std::string &base)
{
    std::string paddingless(base.data(), base.find("="));
    return std::string(base64_to_bin(paddingless.begin()),
                       base64_to_bin(paddingless.end()));
}

} // namespace utils
} // namespace proxy
} // namespace one
