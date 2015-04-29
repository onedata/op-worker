/**
 * @file directIOHelper.cc
 * @author Rafal Slota
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif // linux

#include "directIOHelper.h"

#include "helpers/storageHelperFactory.h"

#include <boost/any.hpp>

#include <dirent.h>
#include <errno.h>
#include <fuse.h>
#include <sys/stat.h>

#include <string>

namespace one {
namespace helpers {

namespace {
inline boost::filesystem::path extractPath(const IStorageHelper::ArgsMap &args)
{
    const auto arg = srvArg(0);
    return args.count(arg)
        ? boost::any_cast<std::string>(args.at(arg)).substr(0, PATH_MAX)
        : boost::filesystem::path{};
}
}

inline boost::filesystem::path DirectIOHelper::root(
    const boost::filesystem::path &path)
{
    return m_rootPath / path;
}

boost::future<struct stat> DirectIOHelper::sh_getattr(
    const boost::filesystem::path &p)
{
    auto promise = std::make_shared<boost::promise<struct stat>>();

    m_workerService.post([=]() {
        struct stat stbuf;
        if (lstat(root(p).c_str(), &stbuf) == -1) {
            setPosixError(promise, errno);
        }
        else {
            promise->set_value(std::move(stbuf));
        }
    });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_access(
    const boost::filesystem::path &p, int mask)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post(
        [=]() { setResult(promise, access, root(p).c_str(), mask); });

    return promise->get_future();
}

boost::future<std::string> DirectIOHelper::sh_readlink(
    const boost::filesystem::path &p)
{
    auto promise = std::make_shared<boost::promise<std::string>>();

    m_workerService.post([=]() {
        std::array<char, 1024> buf;
        const int res = readlink(root(p).c_str(), buf.data(), buf.size() - 1);

        if (res == -1) {
            setPosixError(promise, errno);
        }
        else {
            buf[res] = '\0';
            promise->set_value(buf.data());
        }
    });

    return promise->get_future();
}

boost::future<std::vector<std::string>> DirectIOHelper::sh_readdir(
    const boost::filesystem::path &p, off_t offset, size_t count, CTXRef ctx)
{
    boost::promise<std::vector<std::string>> promise;
    promise.set_exception(makePosixError(ENOTSUP));
    return promise.get_future();
}

boost::future<void> DirectIOHelper::sh_mknod(
    const boost::filesystem::path &p, mode_t mode, dev_t rdev)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post([=]() {
        int res;
        const auto fullPath = root(p);

        /* On Linux this could just be 'mknod(path, mode, rdev)' but this
           is more portable */
        if (S_ISREG(mode)) {
            res = open(fullPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, mode);
            if (res >= 0)
                res = close(res);
        }
        else if (S_ISFIFO(mode)) {
            res = mkfifo(fullPath.c_str(), mode);
        }
        else {
            res = mknod(fullPath.c_str(), mode, rdev);
        }

        if (res == -1) {
            setPosixError(promise, errno);
        }
        else {
            promise->set_value();
        }
    });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_mkdir(
    const boost::filesystem::path &p, mode_t mode)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post(
        [=]() { setResult(promise, mkdir, root(p).c_str(), mode); });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_unlink(const boost::filesystem::path &p)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post(
        [=]() { setResult(promise, unlink, root(p).c_str()); });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_rmdir(const boost::filesystem::path &p)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post([=]() { setResult(promise, rmdir, root(p).c_str()); });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_symlink(
    const boost::filesystem::path &from, const boost::filesystem::path &to)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post([=]() {
        setResult(promise, symlink, root(from).c_str(), root(to).c_str());
    });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_rename(
    const boost::filesystem::path &from, const boost::filesystem::path &to)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post([=]() {
        setResult(promise, rename, root(from).c_str(), root(to).c_str());
    });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_link(
    const boost::filesystem::path &from, const boost::filesystem::path &to)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post([=]() {
        setResult(promise, link, root(from).c_str(), root(to).c_str());
    });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_chmod(
    const boost::filesystem::path &p, mode_t mode)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post(
        [=]() { setResult(promise, chmod, root(p).c_str(), mode); });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_chown(
    const boost::filesystem::path &p, uid_t uid, gid_t gid)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post(
        [=]() { setResult(promise, lchown, root(p).c_str(), uid, gid); });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_truncate(
    const boost::filesystem::path &p, off_t size)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post(
        [=]() { setResult(promise, truncate, root(p).c_str(), size); });

    return promise->get_future();
}

boost::future<int> DirectIOHelper::sh_open(
    const boost::filesystem::path &p, CTXRef ctx)
{
    auto promise = std::make_shared<boost::promise<int>>();

    m_workerService.post([=, &ctx]() {
        const int res = open(root(p).c_str(), ctx.m_ffi.flags);
        if (res == -1) {
            setPosixError(promise, errno);
        }
        else {
            fcntl(res, F_SETFL, O_NONBLOCK);
            ctx.m_ffi.fh = res;
            promise->set_value(res);
        }
    });

    return promise->get_future();
}

boost::future<boost::asio::mutable_buffer> DirectIOHelper::sh_read(
    const boost::filesystem::path &p, boost::asio::mutable_buffer buf,
    off_t offset, CTXRef ctx)
{
    auto promise =
        std::make_shared<boost::promise<boost::asio::mutable_buffer>>();

    m_workerService.post([=, &ctx]() {
        int fd =
            ctx.m_ffi.fh > 0 ? ctx.m_ffi.fh : open(root(p).c_str(), O_RDONLY);
        if (fd == -1) {
            setPosixError(promise, errno);
            return;
        }

        auto res = pread(fd, boost::asio::buffer_cast<char *>(buf),
            boost::asio::buffer_size(buf), offset);
        if (res == -1) {
            setPosixError(promise, errno);
        }
        else {
            promise->set_value(boost::asio::buffer(buf, res));
        }

        if (ctx.m_ffi.fh <= 0) {
            close(fd);
        }
    });

    return promise->get_future();
}

boost::future<int> DirectIOHelper::sh_write(const boost::filesystem::path &p,
    boost::asio::const_buffer buf, off_t offset, CTXRef ctx)
{
    auto promise = std::make_shared<boost::promise<int>>();

    m_workerService.post([=, &ctx]() {
        int fd =
            ctx.m_ffi.fh > 0 ? ctx.m_ffi.fh : open(root(p).c_str(), O_WRONLY);
        if (fd == -1) {
            setPosixError(promise, errno);
            return;
        }

        auto res = pwrite(fd, boost::asio::buffer_cast<const char *>(buf),
            boost::asio::buffer_size(buf), offset);
        if (res == -1) {
            setPosixError(promise, errno);
        }
        else {
            promise->set_value(res);
        }

        if (ctx.m_ffi.fh <= 0) {
            close(fd);
        }
    });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_release(
    const boost::filesystem::path &p, CTXRef ctx)
{
    auto promise = std::make_shared<boost::promise<void>>();

    m_workerService.post([=, &ctx]() {
        if (ctx.m_ffi.fh && close(ctx.m_ffi.fh) == -1) {
            setPosixError(promise, errno);
        }
        else {
            ctx.m_ffi.fh = 0;
            promise->set_value();
        }
    });

    return promise->get_future();
}

boost::future<void> DirectIOHelper::sh_flush(
    const boost::filesystem::path &p, CTXRef ctx)
{
    return boost::make_ready_future();
}

boost::future<void> DirectIOHelper::sh_fsync(
    const boost::filesystem::path &p, int isdatasync, CTXRef ctx)
{
    return boost::make_ready_future();
}

DirectIOHelper::DirectIOHelper(
    const ArgsMap &args, boost::asio::io_service &service)
    : m_rootPath{extractPath(args)}
    , m_workerService{service}
{
}

} // namespace helpers
} // namespace one
