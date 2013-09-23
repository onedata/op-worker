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
#define VOK         "ok"
#define VENOENT     "enoent"
#define VEACCES     "eacces"
#define VEEXIST     "eexist"
#define VEIO        "eio"
#define VENOTSUP    "enotsup"

namespace veil {

    /**
     * errno translator.
     * Translates internal VeilClient error codes (strings) to
     * POSIX error codes. If given string is not valid,
     * EIO is returned.
     * @param verr literal name of POSIX error code
     * @return POSIX error code multiplied by -1
     */
    int translateError(std::string verr);

} // namespace veil

#endif // VEIL_ERRORS_H