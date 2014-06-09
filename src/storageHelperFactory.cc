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

StorageHelperFactory::StorageHelperFactory(boost::shared_ptr<SimpleConnectionPool> connectionPool)
    : m_connectionPool{std::move(connectionPool)}
{
}

StorageHelperFactory::~StorageHelperFactory()
{
}

boost::shared_ptr<IStorageHelper> StorageHelperFactory::getStorageHelper(const string &sh_name, const IStorageHelper::ArgsMap &args) {
    if(sh_name == "DirectIO")
        return boost::shared_ptr<IStorageHelper>(new DirectIOHelper(args));
    else if(sh_name == "ClusterProxy")
        return boost::shared_ptr<IStorageHelper>(new ClusterProxyHelper(m_connectionPool, args));
    else
    {
        return boost::shared_ptr<IStorageHelper>();
    }
}

} // namespace helpers
} // namespace veil
