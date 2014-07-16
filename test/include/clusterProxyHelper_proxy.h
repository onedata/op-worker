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

using namespace veil;
using namespace veil::helpers;

using namespace std;
using namespace veil::protocol::remote_file_management;
using namespace veil::protocol::communication_protocol;

class ProxyClusterProxyHelper
    : public ClusterProxyHelper {
public:
    ProxyClusterProxyHelper(boost::shared_ptr<SimpleConnectionPool> pool, const ArgsMap &args)
        : ClusterProxyHelper(pool, BufferLimits{}, args)
    {
    }

    protocol::communication_protocol::Answer sendCluserMessage(protocol::communication_protocol::ClusterMsg &msg) {
        return ClusterProxyHelper::sendCluserMessage(msg);
    }

    protocol::communication_protocol::ClusterMsg commonClusterMsgSetup(std::string inputType, std::string inputData) {
        return ClusterProxyHelper::commonClusterMsgSetup(inputType, inputData);
    }

    std::string requestMessage(std::string inputType, std::string answerType, std::string inputData) {
        return ClusterProxyHelper::requestMessage(inputType, answerType, inputData);
    } 

    std::string requestAtom(std::string inputType, std::string inputData) {
        return ClusterProxyHelper::requestAtom(inputType, inputData);
    }

    int doWrite(std::string path, const std::string &buf, size_t size, off_t offset, ffi_type ffi)
    {
        return ClusterProxyHelper::doWrite(path, buf, size, offset, ffi);
    }

    int doRead(std::string path, std::string &buf, size_t size, off_t offset, ffi_type ffi)
    {
        return ClusterProxyHelper::doRead(path, buf, size, offset, ffi);
    }


};

#endif // CLUSTER_PROXY_HELPER_PROXY_H
