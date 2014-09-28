/*********************************************************************
*  @author Rafal Slota
*  @copyright (C): 2014 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/


#ifndef base64_h
#define base64_h

namespace one {
namespace proxy {
namespace utils {

/**
 * @brief Encodes given binary data with using Base64 format.
 * @param binary Binary data to encode
 * @return Base64 encoded string
 */
std::string base64_encode(const std::string &binary);


/**
 * @brief Decodes given Base64 string.
 * @param base Base64 encoded string
 * @return Encoded binary
 */
std::string base64_decode(const std::string &base);

} // namespace utils
} // namespace proxy
} // namespace one

#endif // base64_h
