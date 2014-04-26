/**
 * @file veilErrors.hh
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include <errno.h>
#include "veilErrors.h"

namespace veil {

    int translateError(const std::string &verr)
    {
        if(verr == VOK)
            return 0;
        else if(verr == VENOENT)
            return -ENOENT;
        else if(verr == VEACCES)
            return -EACCES;
        else if(verr == VEEXIST)
            return -EEXIST;
        else if(verr == VEIO)
            return -EIO;
        else if(verr == VENOTSUP)
            return -ENOTSUP;
        else if(verr == VENOTEMPTY)
            return -ENOTEMPTY;
        else if(verr == VEREMOTEIO)
#ifdef __gnu_linux__
        return -EREMOTEIO;
#else
        return -EIO;
#endif
        else if(verr == VEPERM)
            return -EPERM;
        else if(verr == VEINVAL)
            return -EINVAL;
        else
            return -EIO;
    }

}
