/**
 * @file IStorageHelper.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_I_STORAGE_HELPER_H
#define HELPERS_I_STORAGE_HELPER_H

#include <fuse.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <asio/buffer.hpp>
#include <boost/any.hpp>
#include <boost/filesystem/path.hpp>

#include <unordered_map>
#include <string>
#include <vector>
#include <memory>
#include <system_error>
#include <future>

#define BOOST_THREAD_PROVIDES_FUTURE
#include <boost/thread.hpp>
#include <boost/thread/future.hpp>

namespace one {
namespace helpers {

struct StorageHelperCTX {

    fuse_file_info &m_ffi;
    uid_t uid;
    gid_t gid;

    StorageHelperCTX(fuse_file_info &ffi)
        : m_ffi(ffi)
        , uid(0)
        , gid(0)
    {
    }

    StorageHelperCTX()
        : m_ffi(local_ffi)
        , uid(0)
        , gid(0)
    {
    }

private:
    fuse_file_info local_ffi = {0};
};

using CTXRef = StorageHelperCTX &;
using ErrorRef = std::system_error;

template<class... T>
using GeneralCallback = std::function<void(T..., ErrorRef)>;
using VoidCallback = GeneralCallback<>;

template <class T> using future_t = std::future<T>;
template <class T> using promise_t = std::promise<T>;

/**
 * The IStorageHelper interface.
 * Base class of all storage helpers. Unifies their interface.
 * All callback have their equivalent in FUSE API and should be used in that
 * matter.
 */
class IStorageHelper {
public:
    using ArgsMap = std::unordered_map<std::string, boost::any>;

    virtual ~IStorageHelper() = default;

    virtual void ash_getattr(CTXRef ctx, const boost::filesystem::path &p,
                                   GeneralCallback<struct stat>) = 0;
    virtual void ash_access(CTXRef ctx,
        const boost::filesystem::path &p, int mask, VoidCallback) = 0;
    virtual void ash_readlink(CTXRef ctx,
        const boost::filesystem::path &p, GeneralCallback<std::string>) = 0;
    virtual void ash_readdir(CTXRef ctx,
        const boost::filesystem::path &p, off_t offset, size_t count,
        GeneralCallback<std::vector<std::string>&>) = 0;
    virtual void ash_mknod(CTXRef ctx,
        const boost::filesystem::path &p, mode_t mode, dev_t rdev, VoidCallback) = 0;
    virtual void ash_mkdir(CTXRef ctx,
        const boost::filesystem::path &p, mode_t mode, VoidCallback) = 0;
    virtual void ash_unlink(CTXRef ctx,const boost::filesystem::path &p, VoidCallback) = 0;
    virtual void ash_rmdir(CTXRef ctx, const boost::filesystem::path &p, VoidCallback) = 0;
    virtual void ash_symlink(CTXRef ctx, const boost::filesystem::path &from,
        const boost::filesystem::path &to, VoidCallback) = 0;
    virtual void ash_rename(CTXRef ctx, const boost::filesystem::path &from,
        const boost::filesystem::path &to, VoidCallback) = 0;
    virtual void ash_link(CTXRef ctx, const boost::filesystem::path &from,
        const boost::filesystem::path &to, VoidCallback) = 0;
    virtual void ash_chmod(CTXRef ctx,
        const boost::filesystem::path &p, mode_t mode, VoidCallback) = 0;
    virtual void ash_chown(CTXRef ctx,
        const boost::filesystem::path &p, uid_t uid, gid_t gid, VoidCallback) = 0;
    virtual void ash_truncate(CTXRef ctx,
        const boost::filesystem::path &p, off_t size, VoidCallback) = 0;

    virtual void ash_open(CTXRef ctx,
        const boost::filesystem::path &p, GeneralCallback<int>) = 0;
    virtual void ash_read(CTXRef ctx,
        const boost::filesystem::path &p, asio::mutable_buffer buf,
        off_t offset, GeneralCallback<asio::mutable_buffer>) = 0;
    virtual void ash_write(CTXRef ctx, const boost::filesystem::path &p,
        asio::const_buffer buf, off_t offset, GeneralCallback<int>) = 0;
    virtual void ash_release(CTXRef ctx,
        const boost::filesystem::path &p, VoidCallback) = 0;
    virtual void ash_flush(CTXRef ctx,
        const boost::filesystem::path &p, VoidCallback) = 0;
    virtual void ash_fsync(CTXRef ctx,
        const boost::filesystem::path &p, int isdatasync, VoidCallback) = 0;

    virtual asio::mutable_buffer sh_read(CTXRef ctx, const boost::filesystem::path &p,
        asio::mutable_buffer buf, off_t offset) = 0;
    virtual int sh_write(CTXRef ctx, const boost::filesystem::path &p,
        asio::const_buffer buf, off_t offset) = 0;

protected:
    template <class T>
    static void setPosixError(std::shared_ptr<promise_t<T>> p, int posixCode)
    {
        p->set_exception(std::make_exception_ptr(makePosixError(posixCode)));
    }

    static std::system_error makePosixError(int posixCode)
    {
        posixCode = posixCode > 0 ? posixCode : -posixCode;
        return std::system_error(posixCode, std::system_category());
    }
};

} // namespace helpers
} // namespace one

#endif // HELPERS_I_STORAGE_HELPER_H
