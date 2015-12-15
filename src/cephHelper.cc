/**
 * @file cephHelper.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephHelper.h"

#include <iostream>

namespace one {
namespace helpers {

const error_t CephHelper::SuccessCode = error_t();

CephHelper::CephHelper(const std::unordered_map<std::string, std::string> &args,
    asio::io_service &service)
    : m_service{service}
{
    int ret = 0;
    ret = m_cluster.init2(
        args.at("user_name").c_str(), args.at("cluster_name").c_str(), 0);
    if (ret < 0)
        throw std::system_error(
            makePosixError(ret), "Couldn't initialize the cluster handle.");
    ret = m_cluster.conf_set("mon host", args.at("mon_host").c_str());
    if (ret < 0)
        throw std::system_error(makePosixError(ret),
            "Couldn't set monitor host configuration variable.");
    ret = m_cluster.conf_set("keyring", args.at("keyring").c_str());
    if (ret < 0)
        throw std::system_error(makePosixError(ret),
            "Couldn't set keyring configuration variable.");
    ret = m_cluster.connect();
    if (ret < 0)
        throw std::system_error(
            makePosixError(ret), "Couldn't connect to cluster.");
    ret = m_cluster.ioctx_create(args.at("pool_name").c_str(), m_ioCtx);
    if (ret < 0)
        throw std::system_error(makePosixError(ret), "Couldn't set up ioctx.");
}

CephHelper::~CephHelper()
{
    m_ioCtx.close();
    m_cluster.shutdown();
}

void CephHelper::ash_unlink(
    CTXRef ctx, const boost::filesystem::path &p, VoidCallback callback)
{
    auto fileId = p.string();
    librados::AioCompletion *completion =
        librados::Rados::aio_create_completion();
    int ret = m_ioCtx.aio_remove(fileId, completion);
    if (ret < 0) {
        completion->release();
        callback(makePosixError(ret));
        return;
    }

    m_service.post([
        =,
        callback = std::move(callback),
        completion = std::move(completion)
    ]() mutable {
        completion->wait_for_complete();
        ret = completion->get_return_value();
        completion->release();
        if (ret < 0)
            callback(makePosixError(ret));
        else
            callback(SuccessCode);
    });
}

void CephHelper::ash_read(CTXRef /*ctx*/, const boost::filesystem::path &p,
    asio::mutable_buffer buf, off_t offset,
    GeneralCallback<asio::mutable_buffer> callback)
{
    auto fileId = p.string();
    auto size = asio::buffer_size(buf);
    auto bl = std::make_shared<librados::bufferlist>(size);
    librados::AioCompletion *completion =
        librados::Rados::aio_create_completion();
    int ret = m_ioCtx.aio_read(fileId, completion, bl.get(), size, offset);
    if (ret < 0) {
        completion->release();
        callback(asio::mutable_buffer(), makePosixError(ret));
        return;
    }

    m_service.post([
        =,
        bl = std::move(bl),
        callback = std::move(callback),
        completion = std::move(completion)
    ]() mutable {
        completion->wait_for_complete();
        ret = completion->get_return_value();
        completion->release();
        if (ret < 0)
            callback(asio::mutable_buffer(), makePosixError(ret));
        else {
            asio::buffer_copy(buf, asio::mutable_buffer(bl->c_str(), ret));
            callback(asio::buffer(buf, ret), SuccessCode);
        }
    });
}

void CephHelper::ash_write(CTXRef /*ctx*/, const boost::filesystem::path &p,
    asio::const_buffer buf, off_t offset, GeneralCallback<std::size_t> callback)
{
    auto fileId = p.string();
    auto size = asio::buffer_size(buf);
    librados::bufferlist bl;
    bl.append(asio::buffer_cast<const char *>(buf));
    librados::AioCompletion *completion =
        librados::Rados::aio_create_completion();
    int ret = m_ioCtx.aio_write(fileId, completion, bl, size, offset);
    if (ret < 0) {
        completion->release();
        callback(0, makePosixError(ret));
        return;
    }

    m_service.post([
        =,
        callback = std::move(callback),
        completion = std::move(completion)
    ]() mutable {
        completion->wait_for_complete();
        ret = completion->get_return_value();
        completion->release();
        if (ret < 0)
            callback(0, makePosixError(ret));
        else
            callback(size, SuccessCode);
    });
}

void CephHelper::ash_truncate(CTXRef ctx, const boost::filesystem::path &p,
    off_t size, VoidCallback callback)
{
    auto fileId = p.string();
    m_service.post([
        =,
        fileId = std::move(fileId),
        callback = std::move(callback)
    ]() mutable {
        int ret = m_ioCtx.trunc(fileId, size);
        if (ret < 0)
            callback(makePosixError(ret));
        else
            callback(SuccessCode);
    });
}

} // namespace helpers
} // namespace one
