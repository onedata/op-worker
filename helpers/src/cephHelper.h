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

#include <rados/librados.hpp>

#include <asio.hpp>

namespace one {
namespace helpers {

class CephHelperCTX : public IStorageHelperCTX {
public:
    CephHelperCTX(std::unordered_map<std::string, std::string> args);

    ~CephHelperCTX();

    void setUserCTX(std::unordered_map<std::string, std::string> args);

    std::unordered_map<std::string, std::string> getUserCTX();

    /**
     * Establishes connection to the Ceph storage cluster.
     * @param reconnect Flag that defines whether close current connection (if
     * present) and establish new one.
     */
    int connect(bool reconnect = false);

    librados::Rados cluster;
    librados::IoCtx ioCTX;

private:
    bool m_connected = false;
    std::unordered_map<std::string, std::string> m_args;
};

class CephHelper : public IStorageHelper {
public:
    CephHelper(std::unordered_map<std::string, std::string> args,
        asio::io_service &service);

    CTXPtr createCTX();

    void ash_open(CTXPtr ctx, const boost::filesystem::path &p,
        std::vector<Flag> flags, GeneralCallback<int> callback)
    {
        callback(0, SUCCESS_CODE);
    }

    void ash_unlink(
        CTXPtr ctx, const boost::filesystem::path &p, VoidCallback callback);

    void ash_read(CTXPtr ctx, const boost::filesystem::path &p,
        asio::mutable_buffer buf, off_t offset,
        GeneralCallback<asio::mutable_buffer>);

    void ash_write(CTXPtr ctx, const boost::filesystem::path &p,
        asio::const_buffer buf, off_t offset, GeneralCallback<std::size_t>);

    void ash_truncate(CTXPtr ctx, const boost::filesystem::path &p, off_t size,
        VoidCallback callback);

    void ash_mknod(CTXPtr ctx, const boost::filesystem::path &p, mode_t mode,
        std::vector<Flag> flags, dev_t rdev, VoidCallback callback)
    {
        callback(SUCCESS_CODE);
    }

    void ash_mkdir(CTXPtr ctx, const boost::filesystem::path &p, mode_t mode,
        VoidCallback callback)
    {
        callback(SUCCESS_CODE);
    }

    void ash_chmod(CTXPtr ctx, const boost::filesystem::path &p, mode_t mode,
        VoidCallback callback)
    {
        callback(SUCCESS_CODE);
    }

private:
    struct UnlinkCallbackData {
        UnlinkCallbackData(std::string _fileId, VoidCallback _callback)
            : fileId{std::move(_fileId)}
            , callback{std::move(_callback)}
        {
        }

        std::string fileId;
        VoidCallback callback;
        librados::AioCompletion *completion;
    };

    struct ReadCallbackData {
        ReadCallbackData(std::string _fileId, std::size_t _size,
            asio::mutable_buffer _buffer,
            GeneralCallback<asio::mutable_buffer> _callback)
            : fileId{std::move(_fileId)}
            , buffer{std::move(_buffer)}
            , callback{std::move(_callback)}
        {
            bufferlist.append(ceph::buffer::create_static(
                _size, asio::buffer_cast<char *>(buffer)));
        }

        std::string fileId;
        librados::bufferlist bufferlist;
        asio::mutable_buffer buffer;
        GeneralCallback<asio::mutable_buffer> callback;
        librados::AioCompletion *completion;
    };

    struct WriteCallbackData {
        WriteCallbackData(std::string _fileId, std::size_t _size,
            asio::const_buffer _buffer, GeneralCallback<std::size_t> _callback)
            : fileId{std::move(_fileId)}
            , size{_size}
            , callback{std::move(_callback)}
        {
            bufferlist.append(asio::buffer_cast<const char *>(_buffer));
        }

        std::string fileId;
        std::size_t size;
        librados::bufferlist bufferlist;
        GeneralCallback<std::size_t> callback;
        librados::AioCompletion *completion;
    };

    std::shared_ptr<CephHelperCTX> getCTX(CTXPtr rawCTX) const;

    asio::io_service &m_service;
    std::unordered_map<std::string, std::string> m_args;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_CEPH_HELPER_H
