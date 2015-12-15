/**
 * @file cephHelper.h
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_CEPH_HELPER_H
#define HELPERS_CEPH_HELPER_H

#include "helpers/IStorageHelper.h"

#include <asio.hpp>
#include <rados/librados.hpp>

namespace one {
namespace helpers {

class CephHelper : public IStorageHelper {
public:
    CephHelper(const std::unordered_map<std::string, std::string> &args,
        asio::io_service &service);

    virtual ~CephHelper();

    void ash_unlink(
        CTXRef ctx, const boost::filesystem::path &p, VoidCallback callback);

    void ash_read(CTXRef ctx, const boost::filesystem::path &p,
        asio::mutable_buffer buf, off_t offset,
        GeneralCallback<asio::mutable_buffer>);

    void ash_write(CTXRef ctx, const boost::filesystem::path &p,
        asio::const_buffer buf, off_t offset, GeneralCallback<std::size_t>);

    void ash_truncate(CTXRef ctx, const boost::filesystem::path &p, off_t size,
        VoidCallback callback);

private:
    asio::io_service &m_service;
    librados::Rados m_cluster;
    librados::IoCtx m_ioCtx;
    static const error_t SuccessCode;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_CEPH_HELPER_H
