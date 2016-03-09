/**
 * @file proxyIOProxy.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "proxyIOHelper.h"
#include "communication/communicator.h"

#include <asio/buffer.hpp>
#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>

#include <string>

using namespace boost::python;

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

class ProxyIOProxy {
public:
    ProxyIOProxy(
        std::string storageId, std::string host, const unsigned short port)
        : m_communicator{1, host, port, false,
              one::communication::createConnection}
        , m_helper{{{"storage_id", storageId}}, m_communicator}
    {
        m_communicator.setScheduler(std::make_shared<one::Scheduler>(1));
        m_communicator.connect();
    }

    int write(
        std::string fileId, std::string data, int offset,
        std::map<std::string, std::string> parameters)
    {
        ReleaseGIL guard;
        auto ctx = std::make_shared<one::helpers::IStorageHelperCTX>();
        return m_helper.sh_write(
            std::move(ctx), fileId, asio::buffer(data), offset, parameters);
    }

    std::string read(
        std::string fileId, int offset, int size,
        std::map<std::string, std::string> parameters)
    {
        ReleaseGIL guard;
        auto ctx = std::make_shared<one::helpers::IStorageHelperCTX>();
        std::string buffer(size, '\0');
        m_helper.sh_read(std::move(ctx), fileId, asio::buffer(buffer), offset,
                         parameters);
        return buffer;
    }

private:
    one::communication::Communicator m_communicator;
    one::helpers::ProxyIOHelper m_helper;
};

namespace {
boost::shared_ptr<ProxyIOProxy> create(
    std::string storageId, std::string host, int port)
{
    return boost::make_shared<ProxyIOProxy>(storageId, host, port);
}
}

std::string raw_read(tuple args, dict kwargs)
{
    std::string fileId = extract<std::string>(args[1]);
    int offset = extract<int>(args[2]);
    int size = extract<int>(args[3]);
    dict parametersDict = extract<dict>(args[4]);

    std::map<std::string, std::string> parametersMap;
    list keys = parametersDict.keys();
    for (int i = 0; i < len(keys); ++i) {
        std::string key = extract<std::string>(keys[i]);
        std::string val = extract<std::string>(parametersDict[key]);
        parametersMap[key] = val;
    }

    return extract<ProxyIOProxy&>(args[0])()
            .read(fileId, offset, size, parametersMap);
}

int raw_write(tuple args, dict kwargs)
{
    std::string fileId = extract<std::string>(args[1]);
    std::string data = extract<std::string>(args[2]);
    int offset = extract<int>(args[3]);
    dict parametersDict = extract<dict>(args[4]);

    std::map<std::string, std::string> parametersMap;
    list keys = parametersDict.keys();
    for (int i = 0; i < len(keys); ++i) {
        std::string key = extract<std::string>(keys[i]);
        std::string val = extract<std::string>(parametersDict[key]);
        parametersMap[key] = val;
    }

    return extract<ProxyIOProxy&>(args[0])()
            .write(fileId, data, offset, parametersMap);
}

BOOST_PYTHON_MODULE(proxy_io)
{
    class_<ProxyIOProxy, boost::noncopyable>("ProxyIOProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("read", raw_function(raw_read))
        .def("write", raw_function(raw_write));
}
