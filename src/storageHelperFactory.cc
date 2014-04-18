/**
 * @file storageHelperFactory.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "helpers/storageHelperFactory.h"
#include "directIOHelper.h"
#include "clusterProxyHelper.h"
#include <boost/algorithm/string.hpp>
#include "communicationHandler.h"

using namespace boost;
using namespace std;

namespace veil {
namespace helpers {

namespace config {

    // Variables below are used as default values when ConnectionPool object is wasnt set
    // but storage helper tries to use TCP/IP connection. It should not happen.
    unsigned int    clusterPort = 0;
    cert_info_fun   getCertInfo;
    string          clusterHostname;


    namespace {
        boost::shared_ptr<SimpleConnectionPool> connectionPool;
    }

    void setConnectionPool(boost::shared_ptr<SimpleConnectionPool> pool)
    {
        connectionPool = pool;
    }

    boost::shared_ptr<SimpleConnectionPool> getConnectionPool()
    {
        if(!connectionPool && getCertInfo && !clusterHostname.empty())
            connectionPool.reset(new SimpleConnectionPool(clusterHostname, clusterPort, getCertInfo));

        return connectionPool;
    }

namespace buffers {

    size_t writeBufferGlobalSizeLimit       = 0;
    size_t readBufferGlobalSizeLimit        = 0;

    size_t writeBufferPerFileSizeLimit      = 0;
    size_t readBufferPerFileSizeLimit       = 0;

    size_t preferedBlockSize                = 4 * 1024;

} // namespace buffers

} // namespace config


namespace utils {

    string tolower(string input) {
        boost::algorithm::to_lower(input);
        return input;
    }

} // namespace utils

StorageHelperFactory::StorageHelperFactory()
{
}

StorageHelperFactory::~StorageHelperFactory()
{
}

boost::shared_ptr<IStorageHelper> StorageHelperFactory::getStorageHelper(std::string sh_name, std::vector<std::string> args) {
    if(sh_name == "DirectIO")
        return boost::shared_ptr<IStorageHelper>(new DirectIOHelper(args));
    else if(sh_name == "ClusterProxy")
        return boost::shared_ptr<IStorageHelper>(new ClusterProxyHelper(args));
    else
    {
        return boost::shared_ptr<IStorageHelper>();
    }
}

} // namespace helpers
} // namespace veil
