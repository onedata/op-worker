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

#ifdef __linux__
#include <sys/fsuid.h>
#endif

#include <map>
#include <string>

namespace one {
namespace helpers {

namespace {
inline boost::filesystem::path extractPath(
    const std::unordered_map<std::string, std::string> &args)
{
    auto it = args.find("root_path");
    if (it == args.end())
        return {};

    return {it->second};
}
}

const error_t DirectIOHelper::SuccessCode = error_t();

inline boost::filesystem::path DirectIOHelper::root(
    const boost::filesystem::path &path)
{
    return m_rootPath / path;
}

#ifdef __linux__
DirectIOHelper::LinuxUserCTX::LinuxUserCTX(PosixHelperCTXPtr helperCTX)
    : uid(helperCTX->uid)
    , gid(helperCTX->gid)
{
    prev_uid = setfsuid(uid);
    prev_gid = setfsgid(gid);

    current_uid = setfsuid(-1);
    current_gid = setfsgid(-1);
}

bool DirectIOHelper::LinuxUserCTX::valid()
{
    return current_uid == uid && current_gid == gid;
}

DirectIOHelper::LinuxUserCTX::~LinuxUserCTX()
{
    setfsuid(prev_uid);
    setfsgid(prev_gid);
}
#endif

bool DirectIOHelper::NoopUserCTX::valid() { return true; }

#ifdef __linux__
DirectIOHelper::UserCTXFactory DirectIOHelper::linuxUserCTXFactory = [](
    DirectIOHelper::PosixHelperCTXPtr ctx) {
    return std::make_unique<DirectIOHelper::LinuxUserCTX>(std::move(ctx));
};
#endif
DirectIOHelper::UserCTXFactory DirectIOHelper::noopUserCTXFactory = [](
    DirectIOHelper::PosixHelperCTXPtr) {
    return std::make_unique<DirectIOHelper::NoopUserCTX>();
};

CTXPtr DirectIOHelper::createCTX()
{
    return std::make_shared<PosixHelperCTX>();
}

void DirectIOHelper::ash_getattr(CTXPtr rawCTX,
    const boost::filesystem::path &p, GeneralCallback<struct stat> callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            struct stat stbuf;

            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(std::move(stbuf), makePosixError(EDOM));
                return;
            }

            if (lstat(root(p).c_str(), &stbuf) == -1) {
                callback(std::move(stbuf), makePosixError(errno));
            }
            else {
                callback(std::move(stbuf), SuccessCode);
            }
        });
}

void DirectIOHelper::ash_access(CTXPtr rawCTX, const boost::filesystem::path &p,
    int mask, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, access, root(p).c_str(), mask);
        });
}

void DirectIOHelper::ash_readlink(CTXPtr rawCTX,
    const boost::filesystem::path &p, GeneralCallback<std::string> callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(std::string(), makePosixError(EDOM));
                return;
            }

            std::array<char, 1024> buf;
            const int res =
                readlink(root(p).c_str(), buf.data(), buf.size() - 1);

            if (res == -1) {
                callback(std::string(), makePosixError(errno));
            }
            else {
                buf[res] = '\0';
                callback(buf.data(), SuccessCode);
            }
        });
}

void DirectIOHelper::ash_readdir(CTXPtr ctx, const boost::filesystem::path &p,
    off_t offset, size_t count,
    GeneralCallback<const std::vector<std::string> &> callback)
{
    std::vector<std::string> ret;
    callback(ret, makePosixError(ENOTSUP));
}

void DirectIOHelper::ash_mknod(CTXPtr rawCTX, const boost::filesystem::path &p,
    mode_t mode, dev_t rdev, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

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
                callback(makePosixError(errno));
            }
            else {
                callback(SuccessCode);
            }
        });
}

void DirectIOHelper::ash_mkdir(CTXPtr rawCTX, const boost::filesystem::path &p,
    mode_t mode, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, mkdir, root(p).c_str(), mode);
        });
}

void DirectIOHelper::ash_unlink(
    CTXPtr rawCTX, const boost::filesystem::path &p, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, unlink, root(p).c_str());
        });
}

void DirectIOHelper::ash_rmdir(
    CTXPtr rawCTX, const boost::filesystem::path &p, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, rmdir, root(p).c_str());
        });
}

void DirectIOHelper::ash_symlink(CTXPtr rawCTX,
    const boost::filesystem::path &from, const boost::filesystem::path &to,
    VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, symlink, root(from).c_str(), root(to).c_str());
        });
}

void DirectIOHelper::ash_rename(CTXPtr rawCTX,
    const boost::filesystem::path &from, const boost::filesystem::path &to,
    VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, rename, root(from).c_str(), root(to).c_str());
        });
}

void DirectIOHelper::ash_link(CTXPtr rawCTX,
    const boost::filesystem::path &from, const boost::filesystem::path &to,
    VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, link, root(from).c_str(), root(to).c_str());
        });
}

void DirectIOHelper::ash_chmod(CTXPtr rawCTX, const boost::filesystem::path &p,
    mode_t mode, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, chmod, root(p).c_str(), mode);
        });
}

void DirectIOHelper::ash_chown(CTXPtr rawCTX, const boost::filesystem::path &p,
    uid_t uid, gid_t gid, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, lchown, root(p).c_str(), uid, gid);
        });
}

void DirectIOHelper::ash_truncate(CTXPtr rawCTX,
    const boost::filesystem::path &p, off_t size, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            setResult(callback, truncate, root(p).c_str(), size);
        });
}

void DirectIOHelper::ash_open(CTXPtr rawCTX, const boost::filesystem::path &p,
    GeneralCallback<int> callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() {
            auto userCTX = m_userCTXFactory(ctx);
            if (!userCTX->valid()) {
                callback(-1, makePosixError(EDOM));
                return;
            }

            int res = open(root(p).c_str(), ctx->flags);
            if (res == -1) {
                callback(-1, makePosixError(errno));
            }
            else {
                ctx->fh = res;
                callback(res, SuccessCode);
            }
        });
}

void DirectIOHelper::ash_read(CTXPtr rawCTX, const boost::filesystem::path &p,
    asio::mutable_buffer buf, off_t offset,
    GeneralCallback<asio::mutable_buffer> callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(ctx);
            if (!userCTX->valid()) {
                callback(asio::mutable_buffer(), makePosixError(EDOM));
                return;
            }

            try {
                auto res = sh_read(std::move(ctx), p, buf, offset);
                callback(res, SuccessCode);
            }
            catch (std::system_error &e) {
                callback(asio::mutable_buffer(), e.code());
            }
        });
}

void DirectIOHelper::ash_write(CTXPtr rawCTX, const boost::filesystem::path &p,
    asio::const_buffer buf, off_t offset, GeneralCallback<std::size_t> callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(ctx);
            if (!userCTX->valid()) {
                callback(0, makePosixError(EDOM));
                return;
            }

            try {
                auto res = sh_write(std::move(ctx), p, buf, offset);
                callback(res, SuccessCode);
            }
            catch (std::system_error &e) {
                callback(0, e.code());
            }
        });
}

void DirectIOHelper::ash_release(
    CTXPtr rawCTX, const boost::filesystem::path &p, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() {
            auto userCTX = m_userCTXFactory(ctx);
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            if (ctx->fh && close(ctx->fh) == -1) {
                callback(makePosixError(errno));
            }
            else {
                ctx->fh = 0;
                callback(SuccessCode);
            }
        });
}

void DirectIOHelper::ash_flush(
    CTXPtr rawCTX, const boost::filesystem::path &p, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            callback(SuccessCode);
        });
}

void DirectIOHelper::ash_fsync(CTXPtr rawCTX, const boost::filesystem::path &p,
    bool isDataSync, VoidCallback callback)
{
    auto ctx = getCTX(std::move(rawCTX));
    m_workerService.post(
        [ =, ctx = std::move(ctx), callback = std::move(callback) ]() mutable {
            auto userCTX = m_userCTXFactory(std::move(ctx));
            if (!userCTX->valid()) {
                callback(makePosixError(EDOM));
                return;
            }

            callback(SuccessCode);
        });
}

std::size_t DirectIOHelper::sh_write(CTXPtr rawCTX,
    const boost::filesystem::path &p, asio::const_buffer buf, off_t offset)
{
    auto ctx = getCTX(std::move(rawCTX));
    auto userCTX = m_userCTXFactory(ctx);
    if (!userCTX->valid()) {
        throw std::system_error(makePosixError(EDOM));
    }

    int fd = ctx->fh > 0 ? ctx->fh : open(root(p).c_str(), O_WRONLY);
    if (fd == -1) {
        throw std::system_error(makePosixError(errno));
    }

    auto res = pwrite(fd, asio::buffer_cast<const char *>(buf),
        asio::buffer_size(buf), offset);

    auto potentialError = makePosixError(errno);

    if (ctx->fh <= 0) {
        close(fd);
    }

    if (res == -1) {
        throw std::system_error(potentialError);
    }

    return res;
}

asio::mutable_buffer DirectIOHelper::sh_read(CTXPtr rawCTX,
    const boost::filesystem::path &p, asio::mutable_buffer buf, off_t offset)
{
    auto ctx = getCTX(std::move(rawCTX));
    auto userCTX = m_userCTXFactory(ctx);
    if (!userCTX->valid()) {
        throw std::system_error(makePosixError(EDOM));
    }

    int fd = ctx->fh > 0 ? ctx->fh : open(root(p).c_str(), O_RDONLY);
    if (fd == -1) {
        throw std::system_error(makePosixError(errno));
    }

    auto res = pread(
        fd, asio::buffer_cast<char *>(buf), asio::buffer_size(buf), offset);

    auto potentialError = makePosixError(errno);

    if (ctx->fh <= 0) {
        close(fd);
    }

    if (res == -1) {
        throw std::system_error(potentialError);
    }

    return std::move(asio::buffer(buf, res));
}

DirectIOHelper::DirectIOHelper(
    const std::unordered_map<std::string, std::string> &args,
    asio::io_service &service, UserCTXFactory userCTXFactory)
    : m_rootPath{extractPath(args)}
    , m_workerService{service}
    , m_userCTXFactory{userCTXFactory}
{
}

std::shared_ptr<PosixHelperCTX> DirectIOHelper::getCTX(CTXPtr rawCTX) const
{
    auto ctx = std::dynamic_pointer_cast<PosixHelperCTX>(rawCTX);
    if (ctx == nullptr)
        throw std::make_error_code(std::errc::invalid_argument);
    return ctx;
}

void PosixHelperCTX::setUserCTX(
    std::unordered_map<std::string, std::string> args)
{
    uid = std::stoi(args.at("uid"));
    gid = std::stoi(args.at("gid"));
}

std::unordered_map<std::string, std::string> PosixHelperCTX::getUserCTX()
{
    return {{"uid", std::to_string(uid)}, {"gid", std::to_string(gid)}};
}

void PosixHelperCTX::setFlags(std::vector<IStorageHelperCTX::Flag> _flags)
{
    flags = 0;
    for (const auto &flag : _flags)
        flags |= getFlagValue(flag);
}

void PosixHelperCTX::setFlags(int _flags) { flags = _flags; }

std::vector<IStorageHelperCTX::Flag> PosixHelperCTX::getFlags()
{
    std::vector<IStorageHelperCTX::Flag> _flags;

    for (auto &flag : openFlagTranslation) {
        if (flags & flag.second) {
            _flags.push_back(flag.first);
        }
    }

    for (auto &flag : openModeTranslation) {
        // Mask only open mode (ACCMODE) and compare by value
        if ((flags & O_ACCMODE) == flag.second) {
            _flags.push_back(flag.first);
        }
    }

    return std::move(_flags);
}

int PosixHelperCTX::getFlagValue(Flag flag)
{
    auto searchResult = openFlagTranslation.find(flag);
    if (searchResult != openFlagTranslation.end())
        return searchResult->second;

    searchResult = openModeTranslation.find(flag);
    if (searchResult != openModeTranslation.end())
        return searchResult->second;

    searchResult = fileTypeTranslation.find(flag);
    if (searchResult != fileTypeTranslation.end())
        return searchResult->second;

    throw std::system_error{std::make_error_code(std::errc::invalid_argument)};
}

const std::map<IStorageHelperCTX::Flag, int>
    PosixHelperCTX::openFlagTranslation = {
        {IStorageHelperCTX::Flag::NONBLOCK, O_NONBLOCK},
        {IStorageHelperCTX::Flag::APPEND, O_APPEND},
        {IStorageHelperCTX::Flag::ASYNC, O_ASYNC},
        {IStorageHelperCTX::Flag::FSYNC, O_FSYNC},
        {IStorageHelperCTX::Flag::NOFOLLOW, O_NOFOLLOW},
        {IStorageHelperCTX::Flag::CREAT, O_CREAT},
        {IStorageHelperCTX::Flag::TRUNC, O_TRUNC},
        {IStorageHelperCTX::Flag::EXCL, O_EXCL}};

const std::map<IStorageHelperCTX::Flag, int>
    PosixHelperCTX::openModeTranslation = {
        {IStorageHelperCTX::Flag::RDONLY, O_RDONLY},
        {IStorageHelperCTX::Flag::WRONLY, O_WRONLY},
        {IStorageHelperCTX::Flag::RDWR, O_RDWR}};

const std::map<IStorageHelperCTX::Flag, int>
    PosixHelperCTX::fileTypeTranslation = {
        {IStorageHelperCTX::Flag::IFREG, S_IFREG},
        {IStorageHelperCTX::Flag::IFCHR, S_IFCHR},
        {IStorageHelperCTX::Flag::IFBLK, S_IFBLK},
        {IStorageHelperCTX::Flag::IFIFO, S_IFIFO},
        {IStorageHelperCTX::Flag::IFSOCK, S_IFSOCK}};

} // namespace helpers
} // namespace one
