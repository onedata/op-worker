/**
 * @file cephProxy.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephHelper.h"

#include <asio/buffer.hpp>
#include <asio/executor_work.hpp>
#include <asio/io_service.hpp>
#include <boost/make_shared.hpp>
#include <boost/python.hpp>

#include <chrono>
#include <future>
#include <string>
#include <thread>

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

class CephProxy {
public:
    CephProxy(std::string monHost, std::string keyring, std::string poolName)
        : m_service{1}
        , m_idleWork{asio::make_work(m_service)}
        , m_worker{[=] { m_service.run(); }}
        , m_helper{{{"user_name", "client.admin"}, {"cluster_name", "ceph"},
                       {"mon_host", std::move(monHost)},
                       {"keyring", std::move(keyring)},
                       {"pool_name", std::move(poolName)}},
              m_service}
    {
    }

    ~CephProxy()
    {
        m_service.stop();
        m_worker.join();
    }

    bool unlink(std::string fileId)
    {
        using namespace std::literals;
        ReleaseGIL guard;
        one::helpers::StorageHelperCTX ctx;
        auto promise = std::make_shared<std::promise<bool>>();
        auto future = promise->get_future();

        auto callback = [promise = std::move(promise)](
            const std::error_code &ec) mutable
        {
            if (ec)
                promise->set_exception(
                    std::make_exception_ptr(std::system_error{ec}));
            else
                promise->set_value(true);
        };

        m_helper.ash_unlink(ctx, fileId, std::move(callback));
        if (future.wait_for(2s) != std::future_status::ready)
            throw std::system_error{std::make_error_code(std::errc::timed_out)};

        return future.get();
    }

    std::string read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        one::helpers::StorageHelperCTX ctx;
        std::string buffer(size, '\0');
        m_helper.sh_read(ctx, fileId, asio::buffer(buffer), offset);
        return buffer;
    }

    int write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;
        one::helpers::StorageHelperCTX ctx;
        return m_helper.sh_write(ctx, fileId, asio::buffer(data), offset);
    }

    bool truncate(std::string fileId, int offset)
    {
        using namespace std::literals;
        ReleaseGIL guard;
        one::helpers::StorageHelperCTX ctx;
        auto promise = std::make_shared<std::promise<bool>>();
        auto future = promise->get_future();

        auto callback = [promise = std::move(promise)](
            const std::error_code &ec) mutable
        {
            if (ec)
                promise->set_exception(
                    std::make_exception_ptr(std::system_error{ec}));
            else
                promise->set_value(true);
        };

        m_helper.ash_truncate(ctx, fileId, offset, std::move(callback));
        if (future.wait_for(2s) != std::future_status::ready)
            throw std::system_error{std::make_error_code(std::errc::timed_out)};

        return future.get();
    }

private:
    asio::io_service m_service;
    asio::executor_work<asio::io_service::executor_type> m_idleWork;
    std::thread m_worker;
    one::helpers::CephHelper m_helper;
};

namespace {
boost::shared_ptr<CephProxy> create(
    std::string monHost, std::string keyring, std::string poolName)
{
    return boost::make_shared<CephProxy>(
        std::move(monHost), std::move(keyring), std::move(poolName));
}
}

BOOST_PYTHON_MODULE(ceph)
{
    class_<CephProxy, boost::noncopyable>("CephProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("delete", &CephProxy::unlink)
        .def("read", &CephProxy::read)
        .def("write", &CephProxy::write)
        .def("truncate", &CephProxy::truncate);
}
