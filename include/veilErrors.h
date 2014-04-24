/**
 * @file veilErrors.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */


#ifndef VEIL_ERRORS_H
#define VEIL_ERRORS_H

#include <iostream>

/// VeilClient error codes
#define VOK             "ok"            // Everything is just great
#define VPUSH           "push"          // Everything is even better - PUSH message from cluster. This error code is used as Answer::answer_status for PUSH messages.
#define VENOENT         "enoent"        // File not found
#define VEACCES         "eacces"        // User doesn't have access to requested resource (e.g. file)
#define VEEXIST         "eexist"        // Given file already exist
#define VEIO            "eio"           // Input/output error - default error code for unknown errors
#define VENOTSUP        "enotsup"       // Operation not supported
#define VENOTEMPTY      "enotempty"     // Directory is not empty
#define VEREMOTEIO      "eremoteio"     // Remote I/O error
#define VEPERM          "eperm"         // Operation not permitted
#define VEINVAL         "einval"        // Invalid argument

/// Cluster's answer status
#define INVALID_FUSE_ID "invalid_fuse_id"
#define NO_USER_FOUND_ERROR "no_user_found_error"
#define NO_CONNECTION_FOR_HANDSHAKE "no_connection_for_handshake"


namespace veil {

    namespace error {
        enum Error {
            SERVER_CERT_VERIFICATION_FAILED
        };
    }

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

#endif // VEIL_ERRORS_H
