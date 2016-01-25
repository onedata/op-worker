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
    ProxyIOProxy(std::string spaceId, std::string storageId, std::string host,
        const unsigned short port)
        : m_communicator{1, host, port, false,
              one::communication::createConnection}
        , m_helper{{{"storage_id", storageId}, {"space_id", spaceId}},
              m_communicator}
    {
        m_communicator.setScheduler(std::make_shared<one::Scheduler>(1));
        m_communicator.connect();
    }

    int write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;
        auto ctx = std::make_shared<one::helpers::IStorageHelperCTX>();
        return m_helper.sh_write(
            std::move(ctx), fileId, asio::buffer(data), offset);
    }

    std::string read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        auto ctx = std::make_shared<one::helpers::IStorageHelperCTX>();
        std::string buffer(size, '\0');
        m_helper.sh_read(std::move(ctx), fileId, asio::buffer(buffer), offset);
        return buffer;
    }

private:
    one::communication::Communicator m_communicator;
    one::helpers::ProxyIOHelper m_helper;
};

namespace {
boost::shared_ptr<ProxyIOProxy> create(
    std::string spaceId, std::string storageId, std::string host, int port)
{
    return boost::make_shared<ProxyIOProxy>(spaceId, storageId, host, port);
}
}

BOOST_PYTHON_MODULE(proxy_io)
{
    class_<ProxyIOProxy, boost::noncopyable>("ProxyIOProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("write", &ProxyIOProxy::write)
        .def("read", &ProxyIOProxy::read);
}
