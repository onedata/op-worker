#ifndef base64_h
#define base64_h

namespace one {
namespace proxy {
namespace utils {
    std::string base64_encode(const std::string &binary);
    std::string base64_decode(const std::string &base);
} // namespace utils
} // namespace proxy
} // namespace one

#endif // base64_h
