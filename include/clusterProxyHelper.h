/**
 * @file ClusterProxyHelper.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef CLUSTER_PROXY_HELPER_H
#define CLUSTER_PROXY_HELPER_H

#include <unistd.h>
#include <fcntl.h>
#include <fuse.h>
#include <vector>
#include <string>
#include "helpers/IStorageHelper.h"
#include "simpleConnectionPool.h"
#include "bufferAgent.h"

#include "remote_file_management.pb.h"
#include "communication_protocol.pb.h"

#define PROTOCOL_VERSION 1
#define RFM_MODULE_NAME "remote_files_manager"
#define RFM_DECODER "remote_file_management"
#define COMMUNICATION_PROTOCOL_DECODER "communication_protocol"

namespace veil {
namespace helpers {

/**
 * The ClusterProxyHelper class
 * Storage helper used to access files through VeilCluster (accessed over TLS protocol).
 */
class ClusterProxyHelper : public IStorageHelper {

    public:
        ClusterProxyHelper(std::vector<std::string>);               ///< This storage helper uses either 0 or 3 arguments. If no arguments are passed, default Veilhelpers connetion pooling will be used.
                                                                    ///< Otherwise first argument shall be cluster's hostname, second - cluster's port and third one - path to peer certificate.
        virtual ~ClusterProxyHelper();

        int sh_getattr(const char *path, struct stat *stbuf) ;
        int sh_access(const char *path, int mask) ;
        int sh_readlink(const char *path, char *buf, size_t size) ;
        int sh_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) ;
        int sh_mknod(const char *path, mode_t mode, dev_t rdev) ;
        int sh_mkdir(const char *path, mode_t mode) ;
        int sh_unlink(const char *path) ;
        int sh_rmdir(const char *path) ;
        int sh_symlink(const char *from, const char *to) ;
        int sh_rename(const char *from, const char *to) ;
        int sh_link(const char *from, const char *to) ;
        int sh_chmod(const char *path, mode_t mode) ;
        int sh_chown(const char *path, uid_t uid, gid_t gid) ;
        int sh_truncate(const char *path, off_t size) ;

        #ifdef HAVE_UTIMENSAT
        int sh_utimens(const char *path, const struct timespec ts[2]);
        #endif // HAVE_UTIMENSAT

        int sh_open(const char *path, struct fuse_file_info *fi) ;
        int sh_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) ;
        int sh_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) ;
        int sh_statfs(const char *path, struct statvfs *stbuf) ;
        int sh_flush(const char *path, struct fuse_file_info *fi) ;
        int sh_release(const char *path, struct fuse_file_info *fi) ;
        int sh_fsync(const char *path, int isdatasync, struct fuse_file_info *fi);

        #ifdef HAVE_POSIX_FALLOCATE
        int sh_fallocate(const char *path, int mode, off_t offset, off_t length, struct fuse_file_info *fi) ;
        #endif // HAVE_POSIX_FALLOCATE

        /* xattr operations are optional and can safely be left unimplemented */
        #ifdef HAVE_SETXATTR
        int sh_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) ;
        int sh_getxattr(const char *path, const char *name, char *value, size_t size) ;
        int sh_listxattr(const char *path, char *list, size_t size) ;
        int sh_removexattr(const char *path, const char *name) ;
        #endif // HAVE_SETXATTR

    protected:
        boost::shared_ptr<SimpleConnectionPool>     m_connectionPool;
        unsigned int      m_clusterPort;
        std::string       m_proxyCert;
        std::string       m_clusterHostname;
        BufferAgent       m_bufferAgent;

        protocol::communication_protocol::Answer sendCluserMessage(protocol::communication_protocol::ClusterMsg &msg);      ///< Sends ClusterMsg to cluster and receives Answer. This function handles connection selection and its releasing.
        protocol::communication_protocol::ClusterMsg commonClusterMsgSetup(std::string inputType, std::string& inputData);   ///< Setups commonly used fields in ClusterMsg for RemoteFileManagement.
        std::string requestMessage(std::string inputType, std::string answerType, std::string& inputData);                   ///< Creates & sends ClusterMsg with given types and input. Response is an serialized message od type "answerType".
        std::string requestAtom(std::string inputType, std::string inputData);                                              ///< Same as requestMessage except it always receives Atom. Return value is an strign value of Atom.

        int doWrite(std::string path, const std::string &buf, size_t, off_t, ffi_type);
        int doRead(std::string path, std::string &buf, size_t, off_t, ffi_type);

};

} // namespace helpers
} // namespace veil

#endif /* CLUSTER_PROXY_HELPER_H */
