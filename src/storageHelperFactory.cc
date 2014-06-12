/**
 * @file storageHelperFactory.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "helpers/storageHelperFactory.h"
#include "directIOHelper.h"
#include "clusterProxyHelper.h"
#include "communicationHandler.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

using namespace boost;
using namespace std;

namespace veil {
namespace helpers {

BufferLimits::BufferLimits(const size_t wgl, const size_t rgl, const size_t wfl,
               const size_t rfl, const size_t pbs)
    : writeBufferGlobalSizeLimit{wgl}
    , readBufferGlobalSizeLimit{rgl}
    , writeBufferPerFileSizeLimit{wfl}
    , readBufferPerFileSizeLimit{rfl}
    , preferedBlockSize{pbs}
{
}

namespace utils {

    string tolower(string input) {
        boost::algorithm::to_lower(input);
        return input;
    }

} // namespace utils

StorageHelperFactory::StorageHelperFactory(boost::shared_ptr<SimpleConnectionPool> connectionPool,
                                           const BufferLimits &limits)
    : m_connectionPool{std::move(connectionPool)}
    , m_limits{limits}
{
}

StorageHelperFactory::~StorageHelperFactory()
{
}

boost::shared_ptr<IStorageHelper> StorageHelperFactory::getStorageHelper(const string &sh_name,
                                                                         const IStorageHelper::ArgsMap &args) {
    if(sh_name == "DirectIO")
        return boost::shared_ptr<IStorageHelper>(new DirectIOHelper(args));
    else if(sh_name == "ClusterProxy")
        return boost::shared_ptr<IStorageHelper>(new ClusterProxyHelper(m_connectionPool, m_limits, args));
    else
    {
        return boost::shared_ptr<IStorageHelper>();
    }
}

string srvArg(const int argno)
{
    return "srv_arg" + boost::lexical_cast<std::string>(argno);
}

} // namespace helpers
} // namespace veil
