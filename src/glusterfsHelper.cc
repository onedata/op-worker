/**
 * @file glusterfsHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2017 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "glusterfsHelper.h"
#include "logging.h"
#include "monitoring/monitoring.h"

#include <boost/algorithm/string.hpp>
#include <folly/String.h>
#include <glog/stl_logging.h>

#include <iostream>
#include <map>
#include <sys/xattr.h>

namespace boost {
namespace filesystem {

path makeRelative(path parent, path child)
{
    LOG_FCALL() << LOG_FARG(parent) << LOG_FARG(child);
    parent = parent.relative_path();
    child = child.relative_path();

    if (parent.empty())
        return child;

    path ret;
    path::const_iterator parentItor(parent.begin());
    path::const_iterator childItor(child.begin());

    // Skip the common part of the paths
    for (; parentItor != parent.end() && childItor != child.end() &&
         *parentItor == *childItor;
         parentItor++, childItor++)
        ;

    // Build a path from the remaining elements in the child path
    for (; childItor != child.end(); childItor++) {
        if (*childItor == "..") {
            ret = ret.parent_path();
        }
        else if (*childItor == ".") {
            ;
        }
        else {
            ret /= *childItor;
        }
    }

    return ret;
}
}
} // namespace boost::filesystem

namespace one {
namespace helpers {

template <typename... Args1, typename... Args2>
inline folly::Future<folly::Unit> setHandleResult(
    int (*fun)(Args2...), glfs_fd_t *fd, Args1 &&... args)
{
    if (fun(fd, std::forward<Args1>(args)...) < 0)
        return one::helpers::makeFuturePosixException(errno);

    return folly::makeFuture();
}

template <typename... Args1, typename... Args2>
inline folly::Future<folly::Unit> setContextResult(
    int (*fun)(Args2...), glfs_t *ctx, Args1 &&... args)
{
    if (fun(ctx, std::forward<Args1>(args)...) < 0)
        return one::helpers::makeFuturePosixException(errno);

    return folly::makeFuture();
}

/**
 * Gluster volume connections cached between helpers and handle instances
 */
static std::map<folly::fbstring, GlusterFSConnection> glusterFSConnections;
static std::mutex connectionMutex;

GlusterFSFileHandle::GlusterFSFileHandle(folly::fbstring fileId,
    std::shared_ptr<GlusterFSHelper> helper, std::shared_ptr<glfs_fd_t> glfsFd,
    uid_t uid, gid_t gid)
    : FileHandle{std::move(fileId)}
    , m_helper{std::move(helper)}
    , m_glfsFd{std::move(glfsFd)}
    , m_uid(uid)
    , m_gid(gid)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid);
}

GlusterFSFileHandle::~GlusterFSFileHandle()
{
    LOG_FCALL();
    if (m_needsRelease.exchange(false)) {
        glfs_setfsuid(m_uid);
        glfs_setfsgid(m_gid);

        if (glfs_close(m_glfsFd.get()) < 0) {
            auto ec = makePosixError(errno);
            LOG(WARNING) << "Failed to release file on GlusterFS filesystem: "
                         << ec.message();
        }
    }
}

folly::Future<folly::IOBufQueue> GlusterFSFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.glusterfs.read");

    return m_helper->connect().then([
        offset, size, glfsFd = m_glfsFd, uid = m_uid, gid = m_gid,
        fileId = m_fileId, timer = std::move(timer),
        s = std::weak_ptr<GlusterFSFileHandle>{shared_from_this()}
    ]() mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::IOBufQueue>(ECANCELED);

        glfs_setfsuid(uid);
        glfs_setfsgid(gid);

        folly::IOBufQueue buffer{folly::IOBufQueue::cacheChainLength()};
        char *raw = static_cast<char *>(buffer.preallocate(size, size).first);

        LOG_DBG(1) << "Attempting to read " << size << " bytes at offset "
                   << offset << " from file " << fileId;

        auto readBytesCount = glfs_pread(glfsFd.get(), raw, size, offset, 0);
        if (readBytesCount < 0) {
            LOG_DBG(1) << "Reading file " << fileId
                       << " failed with error: " << readBytesCount;
            return makeFuturePosixException<folly::IOBufQueue>(readBytesCount);
        }

        buffer.postallocate(readBytesCount);

        LOG_DBG(1) << "Read " << readBytesCount << " from file " << fileId;

        ONE_METRIC_TIMERCTX_STOP(timer, readBytesCount);

        return folly::makeFuture(std::move(buffer));
    });
}

folly::Future<std::size_t> GlusterFSFileHandle::write(
    const off_t offset, folly::IOBufQueue buf)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.glusterfs.write");

    return m_helper->connect().then([
        offset, buf = std::move(buf), glfsFd = m_glfsFd, uid = m_uid,
        fileId = m_fileId, timer = std::move(timer), gid = m_gid,
        s = std::weak_ptr<GlusterFSFileHandle>{shared_from_this()}
    ]() mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<std::size_t>(ECANCELED);

        glfs_setfsuid(uid);
        glfs_setfsgid(gid);

        if (buf.empty())
            return folly::makeFuture<std::size_t>(0);

        auto iov = buf.front()->getIov();
        auto iov_size = iov.size();

        LOG_DBG(1) << "Attempting to write " << iov_size << " bytes at offset "
                   << offset << " to file " << fileId;

        auto res = glfs_pwritev(glfsFd.get(), iov.data(), iov_size, offset, 0);
        if (res == -1) {
            LOG_DBG(1) << "Writing to file " << fileId
                       << " failed with error: " << res;
            return makeFuturePosixException<std::size_t>(errno);
        }

        LOG_DBG(1) << "Written " << res << " bytes to file " << fileId;

        ONE_METRIC_TIMERCTX_STOP(timer, res);

        return folly::makeFuture<std::size_t>(res);
    });
}

const Timeout &GlusterFSFileHandle::timeout()
{
    LOG_FCALL();

    return m_helper->timeout();
}

folly::Future<folly::Unit> GlusterFSFileHandle::release()
{
    LOG_FCALL();

    if (!m_needsRelease.exchange(false))
        return folly::makeFuture();

    return m_helper->connect().then([
        glfsFd = m_glfsFd, uid = m_uid, gid = m_gid,
        s = std::weak_ptr<GlusterFSFileHandle>{shared_from_this()}
    ] {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        glfs_setfsuid(uid);
        glfs_setfsgid(gid);

        LOG_DBG(1) << "Closing file";

        return setHandleResult(glfs_close, glfsFd.get());
    });
}

folly::Future<folly::Unit> GlusterFSFileHandle::flush()
{
    LOG_FCALL();

    return m_helper->connect().then([] { return folly::makeFuture(); });
}

folly::Future<folly::Unit> GlusterFSFileHandle::fsync(bool isDataSync)
{
    LOG_FCALL() << LOG_FARG(isDataSync);

    return m_helper->connect().then([
        glfsFd = m_glfsFd, isDataSync, uid = m_uid, gid = m_gid,
        fileId = m_fileId,
        s = std::weak_ptr<GlusterFSFileHandle>{shared_from_this()}
    ] {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        glfs_setfsuid(uid);
        glfs_setfsgid(gid);

        if (isDataSync) {
            LOG_DBG(1) << "Performing data sync on file " << fileId;
            return setHandleResult(glfs_fdatasync, glfsFd.get());
        }
        else {
            LOG_DBG(1) << "Performing sync on file " << fileId;
            return setHandleResult(glfs_fsync, glfsFd.get());
        }
    });
}

GlusterFSHelper::GlusterFSHelper(boost::filesystem::path mountPoint,
    const uid_t uid, const gid_t gid, folly::fbstring hostname, int port,
    folly::fbstring volume, folly::fbstring transport,
    folly::fbstring xlatorOptions, std::shared_ptr<folly::Executor> executor,
    Timeout timeout)
    : m_mountPoint{std::move(mountPoint)}
    , m_uid{uid}
    , m_gid{gid}
    , m_hostname{std::move(hostname)}
    , m_port{port}
    , m_volume{std::move(volume)}
    , m_transport{std::move(transport)}
    , m_xlatorOptions{std::move(xlatorOptions)}
    , m_executor{std::move(executor)}
    , m_timeout{std::move(timeout)}
{
    LOG_FCALL() << LOG_FARG(mountPoint) << LOG_FARG(uid) << LOG_FARG(gid)
                << LOG_FARG(hostname) << LOG_FARG(port) << LOG_FARG(volume)
                << LOG_FARG(transport) << LOG_FARG(xlatorOptions);
}

folly::Future<folly::Unit> GlusterFSHelper::connect()
{
    LOG_FCALL();

    return folly::via(m_executor.get(), [
        this, s = std::weak_ptr<GlusterFSHelper>{shared_from_this()}
    ] {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException(ECANCELED);

        LOG_DBG(1) << "Attempting to connect to GlusterFS server at: "
                   << m_hostname << " volume: " << m_volume;

        // glfs api allows to set user and group id's per thread only, so we
        // have to set it in each lambda, which can be scheduled on
        // an arbitrary worker thread by Folly executor using glfs_setfsuid()
        // and glfs_setfsgid()
        auto ctxId =
            GlusterFSConnection::generateCtxId(m_hostname, m_port, m_volume);

        std::lock_guard<std::mutex> guard{connectionMutex};

        if (glusterFSConnections.find(ctxId) == glusterFSConnections.end()) {
            glusterFSConnections.insert(
                std::pair<folly::fbstring, GlusterFSConnection>(
                    ctxId, GlusterFSConnection()));
        }

        auto &gfsConnection = glusterFSConnections[ctxId];

        if (gfsConnection.connected) {
            m_glfsCtx = gfsConnection.glfsCtx;
            return folly::makeFuture();
        }

        gfsConnection.glfsCtx = std::shared_ptr<glfs_t>(
            glfs_new(m_volume.c_str()), [](glfs_t *ptr) {
                if (ptr)
                    glfs_fini(ptr);
            });
        m_glfsCtx = gfsConnection.glfsCtx;

        if (!m_glfsCtx) {
            LOG(ERROR) << "Couldn't allocate memory for GlusterFS context";
            return makeFuturePosixException(ENOMEM);
        }

        int ret = glfs_set_volfile_server(
            m_glfsCtx.get(), m_transport.c_str(), m_hostname.c_str(), m_port);
        if (ret != 0) {
            LOG(ERROR) << "Couldn't set the GlusterFS hostname: " << m_hostname;
            return makeFuturePosixException(errno);
        }

        auto xlatorOpts = GlusterFSHelper::parseXlatorOptions(m_xlatorOptions);

        for (auto &xlatorOpt : xlatorOpts) {
            ret = glfs_set_xlator_option(m_glfsCtx.get(), m_volume.c_str(),
                xlatorOpt.first.c_str(), xlatorOpt.second.c_str());
            if (ret < 0) {
                LOG(ERROR) << "Couldn't set GlusterFS "
                           << xlatorOpt.first.toStdString()
                           << " translator option from " + m_xlatorOptions;
                return makeFuturePosixException(EINVAL);
            }
        }

        ret = glfs_init(m_glfsCtx.get());
        if (ret != 0) {
            LOG(ERROR) << "Couldn't initialize GlusterFS connection to "
                          "volume: "
                       << m_volume << " at: " << m_hostname;
            return makeFuturePosixException(errno);
        }

        LOG_DBG(1) << "Successfully connected to GlusterFS at: " << m_hostname;

        gfsConnection.connected = true;

        return folly::makeFuture();
    });
}

folly::Future<FileHandlePtr> GlusterFSHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(flags);

    return connect().then([
        this, timeout = m_timeout, filePath = root(fileId), flags, uid = m_uid,
        gid = m_gid
    ]() {
        // glfs_fd_t implementation details are hidden so we need to
        // specify a custom empty deleter for the shared_ptr.
        // The handle should be closed using glfs_close().
        auto glfsFdDeleter = [](glfs_fd_t *ptr) {};

        glfs_setfsuid(uid);
        glfs_setfsgid(gid);

        LOG_DBG(1) << "Attempting to open file " << filePath;

        std::shared_ptr<glfs_fd_t> glfsFd{nullptr};
        // O_CREAT is not supported in GlusterFS for glfs_open().
        // glfs_creat() has to be used instead for creating files.
        if (flags & O_CREAT) {
            glfsFd.reset(
                glfs_creat(m_glfsCtx.get(), filePath.c_str(), flags,
                    (flags & S_IRWXU) | (flags & S_IRWXG) | (flags & S_IRWXO)),
                glfsFdDeleter);
        }
        else {
            glfsFd.reset(glfs_open(m_glfsCtx.get(), filePath.c_str(), flags),
                glfsFdDeleter);
        }

        if (!glfsFd) {
            LOG_DBG(1) << "Opening file " << filePath << " failed";
            return makeFuturePosixException<FileHandlePtr>(errno);
        }

        auto handle = std::make_shared<GlusterFSFileHandle>(
            filePath.string(), shared_from_this(), glfsFd, uid, gid);

        LOG_DBG(1) << "File " << filePath << " opened";

        return folly::makeFuture<FileHandlePtr>(std::move(handle));
    });
}

folly::Future<struct stat> GlusterFSHelper::getattr(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().then(
        [ this, filePath = root(fileId), uid = m_uid, gid = m_gid ] {
            struct stat stbuf = {};

            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Getting stat for file " << filePath;

            if (glfs_lstat(m_glfsCtx.get(), filePath.c_str(), &stbuf) == -1) {
                LOG_DBG(1) << "Stat failed for file " << filePath
                           << " with error " << errno;
                return makeFuturePosixException<struct stat>(errno);
            }

            LOG_DBG(1) << "Got stat for file " << filePath;

            return folly::makeFuture(stbuf);
        });
}

folly::Future<folly::Unit> GlusterFSHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mask);

    return connect().then(
        [ this, filePath = root(fileId), mask, uid = m_uid, gid = m_gid ] {

            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Checking access to file " << filePath
                       << " with mask " << LOG_OCT(mask);

            return setContextResult(
                glfs_access, m_glfsCtx.get(), filePath.c_str(), mask);
        });
}

folly::Future<folly::fbvector<folly::fbstring>> GlusterFSHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

    return connect().then([
        this, filePath = root(fileId), offset, count, uid = m_uid, gid = m_gid
    ] {
        folly::fbvector<folly::fbstring> ret;
        glfs_fd_t *dir;
        struct dirent *dp;

        glfs_setfsuid(uid);
        glfs_setfsgid(gid);

        LOG_DBG(1) << "Attempting to read directory " << filePath
                   << " starting from entry " << offset;

        dir = glfs_opendir(m_glfsCtx.get(), filePath.c_str());

        if (!dir) {
            LOG_DBG(1) << "Reading directory " << filePath
                       << " failed with error " << errno;
            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                errno);
        }

        int offset_ = offset, count_ = count;
        while ((dp = glfs_readdir(dir)) != NULL && count_ > 0) {
            if (strcmp(dp->d_name, ".") && strcmp(dp->d_name, "..")) {
                if (offset_ > 0) {
                    --offset_;
                }
                else {
                    ret.push_back(folly::fbstring(dp->d_name));
                    --count_;
                }
            }
        }
        glfs_closedir(dir);

        LOG_DBG(1) << "Read directory " << filePath << " with entries "
                   << LOG_VEC(ret);

        return folly::makeFuture<folly::fbvector<folly::fbstring>>(
            std::move(ret));
    });
}

folly::Future<folly::fbstring> GlusterFSHelper::readlink(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().then(
        [ this, filePath = root(fileId), uid = m_uid, gid = m_gid ] {
            constexpr std::size_t maxSize = 1024;
            auto buf = folly::IOBuf::create(maxSize);

            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to read link " << filePath;

            const int res = glfs_readlink(m_glfsCtx.get(), filePath.c_str(),
                reinterpret_cast<char *>(buf->writableData()), maxSize - 1);

            if (res < 0) {
                LOG_DBG(1) << "Reading link " << filePath
                           << " failed with error " << errno;
                return makeFuturePosixException<folly::fbstring>(errno);
            }

            buf->append(res);
            auto target = folly::fbstring(buf->moveToFbString().c_str());

            LOG_DBG(1) << "Link " << filePath
                       << " read successfully - resolves to " << target;

            return folly::makeFuture(std::move(target));
        });
}

folly::Future<folly::Unit> GlusterFSHelper::mknod(const folly::fbstring &fileId,
    const mode_t unmaskedMode, const FlagsSet &flags, const dev_t rdev)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(unmaskedMode)
                << LOG_FARG(flagsToMask(flags));

    const mode_t mode = unmaskedMode | flagsToMask(flags);
    return connect().then([
        this, filePath = root(fileId), mode, rdev, uid = m_uid, gid = m_gid
    ] {
        glfs_setfsuid(uid);
        glfs_setfsgid(gid);

        LOG_DBG(1) << "Attempting to mknod " << filePath << " with mode "
                   << LOG_OCT(mode);

        return setContextResult(
            glfs_mknod, m_glfsCtx.get(), filePath.c_str(), mode, rdev);
    });
}

folly::Future<folly::Unit> GlusterFSHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return connect().then(
        [ this, filePath = root(fileId), mode, uid = m_uid, gid = m_gid ] {
            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to create directory " << filePath
                       << " with mode " << LOG_OCT(mode);

            return setContextResult(
                glfs_mkdir, m_glfsCtx.get(), filePath.c_str(), mode);
        });
}

folly::Future<folly::Unit> GlusterFSHelper::unlink(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);
    return connect().then(
        [ this, filePath = root(fileId), uid = m_uid, gid = m_gid ] {
            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to unlink file " << filePath;

            return setContextResult(
                glfs_unlink, m_glfsCtx.get(), filePath.c_str());
        });
}

folly::Future<folly::Unit> GlusterFSHelper::rmdir(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().then(
        [ this, filePath = root(fileId), uid = m_uid, gid = m_gid ] {
            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to remove directory " << filePath;

            return setContextResult(
                glfs_rmdir, m_glfsCtx.get(), filePath.c_str());
        });
}

folly::Future<folly::Unit> GlusterFSHelper::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return connect().then(
        [ this, from = root(from), to = root(to), uid = m_uid, gid = m_gid ] {
            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to create symbolink link from " << from
                       << " to " << to;

            return setContextResult(
                glfs_symlink, m_glfsCtx.get(), from.c_str(), to.c_str());
        });
}

folly::Future<folly::Unit> GlusterFSHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);
    return connect().then(
        [ this, from = root(from), to = root(to), uid = m_uid, gid = m_gid ] {
            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to rename file from " << from << " to "
                       << to;

            return setContextResult(
                glfs_rename, m_glfsCtx.get(), from.c_str(), to.c_str());
        });
}

folly::Future<folly::Unit> GlusterFSHelper::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return connect().then(
        [ this, from = root(from), to = root(to), uid = m_uid, gid = m_gid ] {
            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to create link from " << from << " to "
                       << to;

            return setContextResult(
                glfs_link, m_glfsCtx.get(), from.c_str(), to.c_str());
        });
}

folly::Future<folly::Unit> GlusterFSHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return connect().then(
        [ this, filePath = root(fileId), mode, uid = m_uid, gid = m_gid ] {
            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to chmod of file " << filePath << " to "
                       << LOG_OCT(mode);

            return setContextResult(
                glfs_chmod, m_glfsCtx.get(), filePath.c_str(), mode);
        });
}

folly::Future<folly::Unit> GlusterFSHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid);

    return connect().then([
        this, filePath = root(fileId), newUid = uid, newGid = gid,
        fsuid = m_uid, fsgid = m_gid
    ] {
        glfs_setfsuid(fsuid);
        glfs_setfsgid(fsgid);

        LOG_DBG(1) << "Attempting to change owner of file " << filePath
                   << " to " << newUid << ":" << newGid;

        return setContextResult(
            glfs_chown, m_glfsCtx.get(), filePath.c_str(), newUid, newGid);
    });
}

folly::Future<folly::Unit> GlusterFSHelper::truncate(
    const folly::fbstring &fileId, const off_t size)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    return connect().then(
        [ this, filePath = root(fileId), size, uid = m_uid, gid = m_gid ] {
            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to truncate file " << filePath
                       << " to size " << size;

            return setContextResult(
                glfs_truncate, m_glfsCtx.get(), filePath.c_str(), size);
        });
}

folly::Future<folly::fbstring> GlusterFSHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    return connect().then(
        [ this, filePath = root(fileId), name, uid = m_uid, gid = m_gid ] {
            constexpr std::size_t initialMaxSize = 1024;
            auto buf = folly::IOBuf::create(initialMaxSize);

            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to get extended attribute " << name
                       << " for file " << filePath;

            int res = glfs_getxattr(m_glfsCtx.get(), filePath.c_str(),
                name.c_str(), reinterpret_cast<char *>(buf->writableData()),
                initialMaxSize - 1);

            // If the initial buffer for xattr value was too small, try again
            // with maximum allowed value
            if (res == -1 && errno == ERANGE) {
                buf = folly::IOBuf::create(XATTR_SIZE_MAX);
                res = glfs_getxattr(m_glfsCtx.get(), filePath.c_str(),
                    name.c_str(), reinterpret_cast<char *>(buf->writableData()),
                    XATTR_SIZE_MAX - 1);
            }

            if (res == -1) {
                LOG_DBG(1) << "Getting extended attribute " << name
                           << " for file " << filePath << " failed with error "
                           << errno;
                return makeFuturePosixException<folly::fbstring>(errno);
            }

            buf->append(res);

            LOG_DBG(1) << "Got extended attribute " << name << " for file "
                       << filePath;

            return folly::makeFuture(buf->moveToFbString());
        });
}

folly::Future<folly::Unit> GlusterFSHelper::setxattr(
    const folly::fbstring &fileId, const folly::fbstring &name,
    const folly::fbstring &value, bool create, bool replace)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name) << LOG_FARG(value)
                << LOG_FARG(create) << LOG_FARG(replace);

    return connect().then([
        this, filePath = root(fileId), name, value, create, replace,
        uid = m_uid, gid = m_gid
    ] {
        int flags = 0;

        LOG_DBG(1) << "Attempting to set extended attribute " << name
                   << " for file " << filePath;

        if (create && replace) {
            return makeFuturePosixException<folly::Unit>(EINVAL);
        }
        if (create) {
            flags = XATTR_CREATE;
        }
        else if (replace) {
            flags = XATTR_REPLACE;
        }

        glfs_setfsuid(uid);
        glfs_setfsgid(gid);

        return setContextResult(glfs_setxattr, m_glfsCtx.get(),
            filePath.c_str(), name.c_str(), value.c_str(), value.size(), flags);
    });
}

folly::Future<folly::Unit> GlusterFSHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    return connect().then(
        [ this, filePath = root(fileId), name, uid = m_uid, gid = m_gid ] {
            glfs_setfsuid(uid);
            glfs_setfsgid(gid);

            LOG_DBG(1) << "Attempting to remove extended attribute " << name
                       << " from file " << filePath;

            return setContextResult(glfs_removexattr, m_glfsCtx.get(),
                filePath.c_str(), name.c_str());
        });
}

folly::Future<folly::fbvector<folly::fbstring>> GlusterFSHelper::listxattr(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().then([
        this, filePath = root(fileId), uid = m_uid, gid = m_gid
    ] {
        folly::fbvector<folly::fbstring> ret;

        glfs_setfsuid(uid);
        glfs_setfsgid(gid);

        LOG_DBG(1) << "Attempting to list extended attributes for file "
                   << filePath;

        ssize_t buflen =
            glfs_listxattr(m_glfsCtx.get(), filePath.c_str(), NULL, 0);
        if (buflen == -1)
            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                errno);

        if (buflen == 0)
            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::move(ret));

        auto buf = std::unique_ptr<char[]>(new char[buflen]);
        buflen = glfs_listxattr(
            m_glfsCtx.get(), filePath.c_str(), buf.get(), buflen);

        if (buflen == -1)
            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                errno);

        char *xattrNamePtr = buf.get();
        while (xattrNamePtr < buf.get() + buflen) {
            ret.emplace_back(xattrNamePtr);
            xattrNamePtr +=
                strnlen(xattrNamePtr, buflen - (buf.get() - xattrNamePtr)) + 1;
        }

        LOG_DBG(1) << "Got extended attributes for " << filePath
                   << " with names " << LOG_VEC(ret);

        return folly::makeFuture<folly::fbvector<folly::fbstring>>(
            std::move(ret));
    });
}

GlusterFSXlatorOptions GlusterFSHelper::parseXlatorOptions(
    const folly::fbstring &options)
{
    LOG_FCALL() << LOG_FARG(options);

    GlusterFSXlatorOptions result;

    constexpr auto xlatorOptionsSeparator = ';';
    constexpr auto xlatorOptionValueSeparator = '=';

    if (options.empty())
        return result;

    std::vector<folly::fbstring> optionPairs;
    folly::split(xlatorOptionsSeparator, options, optionPairs);

    for (auto optionPair : optionPairs) {
        if (optionPair.empty())
            continue;

        std::vector<folly::fbstring> optionPairVec;

        folly::split(xlatorOptionValueSeparator, optionPair, optionPairVec);
        if (optionPairVec.size() != 2 || optionPairVec[0].empty() ||
            optionPairVec[1].empty()) {
            throw std::runtime_error(
                std::string("Invalid GlusterFS xlator option: ") +
                options.toStdString());
        }

        result.emplace_back(std::make_pair(optionPairVec[0], optionPairVec[1]));
    }

    return result;
}

boost::filesystem::path GlusterFSHelper::root(
    const folly::fbstring &fileId) const
{
    LOG_FCALL() << LOG_FARG(fileId);

    return m_mountPoint / fileId.toStdString();
}

boost::filesystem::path GlusterFSHelper::relative(
    const folly::fbstring &fileId) const
{
    LOG_FCALL() << LOG_FARG(fileId);

    return boost::filesystem::makeRelative(m_mountPoint, fileId.toStdString());
}

} // namespace helpers
} // namespace one
