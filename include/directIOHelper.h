/**
 * @file directIOHelper.h
 * @author Beata Skiba
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef HELPERS_DIRECT_IO_HELPER_H
#define HELPERS_DIRECT_IO_HELPER_H


#include "helpers/IStorageHelper.h"

#include <boost/filesystem/path.hpp>

#include <fuse.h>
#include <sys/types.h>

namespace one
{
namespace helpers
{

/**
 * The DirectIOHelper class
 * Storage helper used to access files on mounted as local filesystem.
 */
class DirectIOHelper: public IStorageHelper
{
public:
    /**
     * This storage helper uses only the first element of args map.
     * It shall be ablosute path to diretory used by this storage helper as
     * root mount point.
     */
    DirectIOHelper(const ArgsMap&);

    boost::shared_future<struct stat> sh_getattr(const boost::filesystem::path &p);
    boost::shared_future<int> sh_access(const boost::filesystem::path &p, int mask);
    boost::shared_future<std::string> sh_readlink(const boost::filesystem::path &p);
    boost::shared_future<std::vector<std::string>>
            sh_readdir(const boost::filesystem::path &p, off_t offset, size_t count, StorageHelperCTX &ctx);
    boost::shared_future<int> sh_mknod(const boost::filesystem::path &p, mode_t mode, dev_t rdev);
    boost::shared_future<int> sh_mkdir(const boost::filesystem::path &p, mode_t mode);
    boost::shared_future<int> sh_unlink(const boost::filesystem::path &p);
    boost::shared_future<int> sh_rmdir(const boost::filesystem::path &p);
    boost::shared_future<int>
            sh_symlink(const boost::filesystem::path &from, const boost::filesystem::path &to);
    boost::shared_future<int>
            sh_rename(const boost::filesystem::path &from, const boost::filesystem::path &to);
    boost::shared_future<int>
            sh_link(const boost::filesystem::path &from, const boost::filesystem::path &to);
    boost::shared_future<int> sh_chmod(const boost::filesystem::path &p, mode_t mode);
    boost::shared_future<int> sh_chown(const boost::filesystem::path &p, uid_t uid, gid_t gid);
    boost::shared_future<int> sh_truncate(const boost::filesystem::path &p, off_t size);


    boost::shared_future<int> sh_open(const boost::filesystem::path &p, StorageHelperCTX &ctx);
    boost::shared_future<boost::asio::mutable_buffer>
            sh_read(const boost::filesystem::path &p, boost::asio::mutable_buffer buf, off_t offset,
                    StorageHelperCTX &ctx);
    boost::shared_future<int>
            sh_write(const boost::filesystem::path &p, boost::asio::const_buffer buf, off_t offset,
                     StorageHelperCTX &ctx);
    boost::shared_future<int> sh_release(const boost::filesystem::path &p, StorageHelperCTX &ctx);
    boost::shared_future<int> sh_flush(const boost::filesystem::path &p, StorageHelperCTX &ctx);
    boost::shared_future<int>
            sh_fsync(const boost::filesystem::path &p, int isdatasync, StorageHelperCTX &ctx);

private:
    boost::filesystem::path root(const boost::filesystem::path &path);

    const boost::filesystem::path m_rootPath;
};

} // namespace helpers
} // namespace one


#endif // HELPERS_DIRECT_IO_HELPER_H
