/**
 * @file base64.h
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef BASE64_H
#define BASE64_H

#include <string>
#include <vector>

namespace one {
namespace proxy {
namespace utils {

/**
 * Encodes given binary data with using Base64 format.
 * @param binary Binary data to encode
 * @return Base64 encoded string
 */
std::string base64_encode(const std::vector<unsigned char> &binary);


/**
 * Decodes given Base64 string.
 * @param base Base64 encoded string
 * @return Encoded binary
 */
std::string base64_decode(const std::vector<unsigned char> &base);

} // namespace utils
} // namespace proxy
} // namespace one

#endif // BASE64_H
