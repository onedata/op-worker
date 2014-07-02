/**
 * @file storageHelperFactory.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef STORAGE_HELPER_FACTORY_H
#define STORAGE_HELPER_FACTORY_H

#include "helpers/IStorageHelper.h"

#include "simpleConnectionPool.h"

#include <boost/any.hpp>
#include <boost/atomic.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread_time.hpp>

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>

#define PROTOCOL_VERSION 1


namespace veil {
namespace helpers {

    std::string srvArg(const int argno);

    struct BufferLimits
    {
        BufferLimits(const size_t wgl = 0, const size_t rgl = 0, const size_t wfl = 0,
               const size_t rfl = 0, const size_t pbs = 4 * 1024);

        const size_t writeBufferGlobalSizeLimit;
        const size_t readBufferGlobalSizeLimit;

        const size_t writeBufferPerFileSizeLimit;
        const size_t readBufferPerFileSizeLimit;

        const size_t preferedBlockSize;
    };

namespace utils {

    std::string tolower(std::string input);

    template<typename T>
    T fromString(const std::string &in) {
        T out;
        std::istringstream iss(in);
        iss >> out;
        return out;
    }

    template<typename T>
    T mtime()
    {
        boost::posix_time::ptime time_t_epoch(boost::gregorian::date(1970,1,1));
        boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
        boost::posix_time::time_duration diff = now - time_t_epoch;

        return diff.total_milliseconds();
    }

} // namespace utils

/**
 * Factory providing objects of requested storage helpers.
 */
class StorageHelperFactory {
public:
    StorageHelperFactory() = default;
    StorageHelperFactory(boost::shared_ptr<SimpleConnectionPool> connectionPool,
                         const BufferLimits &limits);
    virtual ~StorageHelperFactory() = default;

    /**
     * Produces storage helper object.
     * @param sh Name of storage helper that has to be returned.
     * @param args Arguments vector passed as argument to storge helper's constructor.
     * @return Pointer to storage helper object along with its ownership.
     */
    virtual boost::shared_ptr<IStorageHelper> getStorageHelper(const std::string &sh,
                                                               const IStorageHelper::ArgsMap &args);

private:
    const boost::shared_ptr<SimpleConnectionPool> m_connectionPool;
    const BufferLimits m_limits;
};

} // namespace helpers
} // namespace veil

#endif // STORAGE_HELPER_FACTORY_H
