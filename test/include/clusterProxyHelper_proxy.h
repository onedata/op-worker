/**
 * @file clusterProxyHelper_proxy.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef CLUSTER_PROXY_HELPER_PROXY_H
#define CLUSTER_PROXY_HELPER_PROXY_H


#include "clusterProxyHelper.h"

#include "simpleConnectionPool.h"

#include <memory>

class ProxyClusterProxyHelper: public veil::helpers::ClusterProxyHelper {
public:
    ProxyClusterProxyHelper(std::shared_ptr<veil::SimpleConnectionPool> pool, const ArgsMap &args)
        : ClusterProxyHelper{std::move(pool), veil::helpers::BufferLimits{}, args}
    {
    }

    using veil::helpers::ClusterProxyHelper::sendClusterMessage;
    using veil::helpers::ClusterProxyHelper::commonClusterMsgSetup;
    using veil::helpers::ClusterProxyHelper::requestMessage;
    using veil::helpers::ClusterProxyHelper::requestAtom;
    using veil::helpers::ClusterProxyHelper::doWrite;
    using veil::helpers::ClusterProxyHelper::doRead;
};


#endif // CLUSTER_PROXY_HELPER_PROXY_H
