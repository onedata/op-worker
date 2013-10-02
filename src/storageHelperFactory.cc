/**
 * @file storageHelperFactory.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "helpers/storageHelperFactory.h"
#include "directIOHelper.h"
#include "clusterProxyHelper.h"

using namespace boost;
using namespace std;

namespace veil {
namespace helpers {

namespace config {

    // Variables below are used as default values when ConnectionPool object is wasnt set 
    // but storage helper tries to use TCP/IP connection. It should not happen.
    unsigned int clusterPort;
    string       proxyCert;
    string       clusterHostname;

    namespace {
        boost::shared_ptr<SimpleConnectionPool> connectionPool;
    }

    void setConnectionPool(boost::shared_ptr<SimpleConnectionPool> pool) 
    {
        connectionPool = pool;
    }

    boost::shared_ptr<SimpleConnectionPool> getConnectionPool()
    {
        if(!connectionPool)
            connectionPool.reset(new SimpleConnectionPool(clusterHostname, clusterPort, proxyCert, NULL));

        return connectionPool;
    }

} // namespace config   


StorageHelperFactory::StorageHelperFactory() 
{
}

StorageHelperFactory::~StorageHelperFactory() 
{
}

shared_ptr<IStorageHelper> StorageHelperFactory::getStorageHelper(std::string sh_name, std::vector<std::string> args) {
    if(sh_name == "DirectIO")
        return shared_ptr<IStorageHelper>(new DirectIOHelper(args));
    else if(sh_name == "ClusterProxy")
        return shared_ptr<IStorageHelper>(new ClusterProxyHelper(args));
    else
    {
        return shared_ptr<IStorageHelper>();
    }
}

} // namespace helpers
} // namespace veil