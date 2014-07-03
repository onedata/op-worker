/**
 * @file veilErrors.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_VEIL_ERRORS_H
#define VEILHELPERS_VEIL_ERRORS_H


#include <string>

/// VeilClient error codes
constexpr const char
    *VOK        = "ok",            // Everything is just great
    *VPUSH      = "push",          // Everything is even better - PUSH message from cluster. This error code is used as Answer::answer_status for PUSH messages.
    *VENOENT    = "enoent",        // File not found
    *VEACCES    = "eacces",        // User doesn't have access to requested resource (e.g. file)
    *VEEXIST    = "eexist",        // Given file already exist
    *VEIO       = "eio",           // Input/output error - default error code for unknown errors
    *VENOTSUP   = "enotsup",       // Operation not supported
    *VENOTEMPTY = "enotempty",     // Directory is not empty
    *VEREMOTEIO = "eremoteio",     // Remote I/O error
    *VEPERM     = "eperm",         // Operation not permitted
    *VEINVAL    = "einval";        // Invalid argument

/// Cluster's answer status
constexpr const char
    *INVALID_FUSE_ID                = "invalid_fuse_id",
    *NO_USER_FOUND_ERROR            = "no_user_found_error",
    *NO_CONNECTION_FOR_HANDSHAKE    = "no_connection_for_handshake";

namespace veil
{
namespace error
{

enum Error
{
    SERVER_CERT_VERIFICATION_FAILED
};

} // namespace error

/**
 * errno translator.
 * Translates internal VeilClient error codes (strings) to
 * POSIX error codes. If given string is not valid,
 * EIO is returned.
 * @param verr literal name of POSIX error code
 * @return POSIX error code multiplied by -1
 */
int translateError(const std::string &verr);

} // namespace veil


#endif // VEILHELPERS_VEIL_ERRORS_H
