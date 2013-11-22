/**
 * @file storageHelperFactory.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef STORAGE_HELPER_FACTORY_H
#define STORAGE_HELPER_FACTORY_H 

#include <memory>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include "helpers/IStorageHelper.h"
#include "simpleConnectionPool.h"

#define PROTOCOL_VERSION 1


namespace veil {
namespace helpers {

namespace config {

    extern unsigned int clusterPort;
    extern std::string  proxyCert;
    extern std::string  clusterHostname;

    namespace {
        extern boost::shared_ptr<SimpleConnectionPool> connectionPool;
    }

    void setConnectionPool(boost::shared_ptr<SimpleConnectionPool> pool);
    boost::shared_ptr<SimpleConnectionPool> getConnectionPool();


} // namespace config   

namespace utils {

    std::string tolower(std::string input);
    
    template<typename T>
    T fromString(std::string in) {
        T out;
        std::istringstream iss(in);
        iss >> out;
        return out;
    }
    
} // namespace utils

/**
 * Factory providing objects of requested storage helpers.
 */
class StorageHelperFactory {

	public:

        StorageHelperFactory();
		virtual ~StorageHelperFactory();	

        /**
         * Produces storage helper object.
         * @param sh Name of storage helper that has to be returned.
         * @param args Arguments vector passed as argument to storge helper's constructor.
         * @return Pointer to storage helper object along with its ownership.
         */
        virtual boost::shared_ptr<IStorageHelper> getStorageHelper(std::string sh, std::vector<std::string> args);
};

} // namespace helpers   
} // namespace veil

#endif // STORAGE_HELPER_FACTORY_H
