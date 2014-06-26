/**
 * @file veilErrors.hh
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "veilErrors.h"

#include <errno.h>

#include <unordered_map>

namespace veil
{

int translateError(const std::string &verr)
{
    static const std::unordered_map<std::string, int> errorMap{
        {VOK,           0},
        {VENOENT,       -ENOENT},
        {VEACCES,       -EACCES},
        {VEEXIST,       -EEXIST},
        {VEIO,          -EIO},
        {VENOTSUP,      -ENOTSUP},
        {VENOTEMPTY,    -ENOTEMPTY},
        {VEPERM,        -EPERM},
        {VEINVAL,       -EINVAL},
#ifdef __gnu_linux__
        {VEREMOTEIO,    -EREMOTEIO},
#else
        {VEREMOTEIO,    -EIO},
#endif
    };

    const auto it = errorMap.find(verr);
    return it == errorMap.end() ? -EIO : it->second;
}

}
