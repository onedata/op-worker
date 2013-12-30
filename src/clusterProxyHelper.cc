/**
 * @file ClusterProxyHelper.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif /* linux */

#include <fuse.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <cstring>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include <limits.h>

#include <boost/algorithm/string.hpp>
#include "glog/logging.h"
#include "clusterProxyHelper.h"
#include "helpers/storageHelperFactory.h"
#include <google/protobuf/descriptor.h>
#include "veilErrors.h"

#include <iostream>

using namespace std;
using namespace boost::algorithm;
using namespace veil::protocol::remote_file_management;
using namespace veil::protocol::communication_protocol;

namespace veil {
namespace helpers {


ClusterMsg ClusterProxyHelper::commonClusterMsgSetup(string inputType, string inputData) {

    RemoteFileMangement rfm;
    rfm.set_message_type(utils::tolower(inputType));
    rfm.set_input(inputData);

    ClusterMsg clm;
    clm.set_protocol_version(PROTOCOL_VERSION);
    clm.set_synch(true);
    clm.set_module_name(RFM_MODULE_NAME);
    clm.set_message_decoder_name(RFM_DECODER);
    clm.set_message_type(utils::tolower(rfm.GetDescriptor()->name()));

    clm.set_input(rfm.SerializeAsString());

    return clm;
}

string ClusterProxyHelper::requestMessage(string inputType, string answerType, string inputData) {
    ClusterMsg clm = commonClusterMsgSetup(inputType, inputData);

    clm.set_answer_type(utils::tolower(answerType));
    clm.set_answer_decoder_name(RFM_DECODER);

    Answer answer = sendCluserMessage(clm);

    return answer.worker_answer();
}   

string ClusterProxyHelper::requestAtom(string inputType, string inputData) {
    ClusterMsg clm = commonClusterMsgSetup(inputType, inputData);

    clm.set_answer_type(utils::tolower(Atom::descriptor()->name()));
    clm.set_answer_decoder_name(COMMUNICATION_PROTOCOL_DECODER);

    Answer answer = sendCluserMessage(clm);

    Atom atom;
    if(answer.has_worker_answer()) {
        atom.ParseFromString(answer.worker_answer());
        return atom.value();
    }

    return "";
}

Answer ClusterProxyHelper::sendCluserMessage(ClusterMsg &msg) {
    boost::shared_ptr<CommunicationHandler> connection = m_connectionPool ? m_connectionPool->selectConnection(SimpleConnectionPool::DATA_POOL) : config::getConnectionPool()->selectConnection();
    if(!connection) 
    {
        LOG(ERROR) << "Cannot select connection from connectionPool";
        return Answer();
    }

    Answer answer = connection->communicate(msg, 2);
    if(answer.answer_status() != VEIO)
        config::getConnectionPool()->releaseConnection(connection);

    if(answer.answer_status() != VOK) 
        LOG(WARNING) << "Cluster send non-ok message. status = " << answer.answer_status();

    return answer;
}


//////////////////////
// Helper callbacks //
//////////////////////

int ClusterProxyHelper::sh_getattr(const char *path, struct stat *stbuf)
{
    // Just leave defaults and ignore this call
    return 0;
}

int ClusterProxyHelper::sh_access(const char *path, int mask)
{
    // We dont need this method, return success
    return 0;
}

int ClusterProxyHelper::sh_mknod(const char *path, mode_t mode, dev_t rdev)
{
    LOG(INFO) << "CluserProxyHelper mknod(path: " << string(path) << ")";

    CreateFile msg;
    msg.set_file_id(string(path));
    
    return translateError(requestAtom(msg.GetDescriptor()->name(), msg.SerializeAsString()));
}

int ClusterProxyHelper::sh_unlink(const char *path)
{
    LOG(INFO) << "CluserProxyHelper unlink(path: " << string(path) << ")";

    DeleteFileAtStorage msg;
    msg.set_file_id(string(path));
    
    return translateError(requestAtom(msg.GetDescriptor()->name(), msg.SerializeAsString()));
}

int ClusterProxyHelper::sh_chmod(const char *path, mode_t mode)
{
    return 0;
}

int ClusterProxyHelper::sh_chown(const char *path, uid_t uid, gid_t gid)
{
    return 0;
}

int ClusterProxyHelper::sh_truncate(const char *path, off_t size)
{
    LOG(INFO) << "CluserProxyHelper truncate(path: " << string(path) << ", size: " << size << ")";

    TruncateFile msg;
    msg.set_file_id(string(path));
    msg.set_length(size);
    
    return translateError(requestAtom(msg.GetDescriptor()->name(), msg.SerializeAsString()));
}

int ClusterProxyHelper::sh_open(const char *path, struct fuse_file_info *fi)
{
    LOG(INFO) << "CluserProxyHelper open(path: " << string(path) << ")";

    return m_bufferAgent.onOpen(string(path), fi);
}

int ClusterProxyHelper::sh_read(const char *path, char *buf, size_t size, off_t offset,
            struct fuse_file_info *fi)
{
    LOG(INFO) << "CluserProxyHelper read(path: " << string(path) << ", size: " << size << ", offset: " << offset << ")";

    ReadFile msg;
    msg.set_file_id(string(path));
    msg.set_size(size);
    msg.set_offset(offset);

    FileData answer;

    if(!answer.ParseFromString(
        requestMessage(msg.GetDescriptor()->name(), answer.GetDescriptor()->name(), msg.SerializeAsString())))
    {
        LOG(WARNING) << "Cannot parse answer for file: " << string(path);
        return translateError(VEIO);
    }

    LOG(INFO) << "CluserProxyHelper read answer_status: " << answer.answer_status() << ", read real size: " << answer.data().size();
 
    if(answer.answer_status() == VOK) {
        size_t readSize = (answer.data().size() > size ? size : answer.data().size());

        memcpy(buf, answer.data().data(), readSize);

        if(answer.data().size() != size)
            LOG(WARNING) << "read for file: " << string(path) << " returned " << answer.data().size() << "bytes. Expected: " << size;

        return readSize;

    } else if(answer.answer_status() == "ok:TODO2") {
        /// TODO: implement big read
        LOG(ERROR) << "Cluster requested to read file (" << string(path) << ") directly over TCP/IP which is not implemented yet";
        return -ENOTSUP;
    } else
        return translateError(answer.answer_status());
}

int ClusterProxyHelper::sh_write(const char *path, const char *buf, size_t size,
             off_t offset, struct fuse_file_info *fi)
{
    //LOG(INFO) << "CluserProxyHelper write(path: " << string(path) << ", size: " << size << ", offset: " << offset << ")";
    
    return m_bufferAgent.onWrite(string(path), string(buf, size), size, offset, fi);
}

int ClusterProxyHelper::sh_release(const char *path, struct fuse_file_info *fi)
{
    LOG(INFO) << "CluserProxyHelper release(path: " << string(path) << ")";
    return m_bufferAgent.onRelease(string(path), fi);;
}

int ClusterProxyHelper::sh_flush(const char *path, struct fuse_file_info *fi)
{
    LOG(INFO) << "CluserProxyHelper flush(path: " << string(path) << ")";
    return m_bufferAgent.onFlush(string(path), fi);
}

int ClusterProxyHelper::sh_fsync(const char *path, int isdatasync,
             struct fuse_file_info *fi)
{
    /* Just a stub.     This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) isdatasync;
    (void) fi;
    return 0;
}

int ClusterProxyHelper::sh_mkdir(const char *path, mode_t mode)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
               off_t offset, struct fuse_file_info *fi)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_statfs(const char *path, struct statvfs *stbuf)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_rmdir(const char *path)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_symlink(const char *from, const char *to)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_rename(const char *from, const char *to)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_link(const char *from, const char *to)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_readlink(const char *path, char *buf, size_t size)
{
    return ENOTSUP;
}

#ifdef HAVE_POSIX_FALLOCATE
int ClusterProxyHelper::sh_fallocate(const char *path, int mode,
            off_t offset, off_t length, struct fuse_file_info *fi)
{
    return ENOTSUP;
}
#endif  /* HAVE_POSIX_FALLOCATE */

#ifdef HAVE_UTIMENSAT
int ClusterProxyHelper::sh_utimens(const char *path, const struct timespec ts[2])
{
    return 0;
}
#endif /* HAVE_UTIMENSAT */

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
int ClusterProxyHelper::sh_setxattr(const char *path, const char *name, const char *value,
            size_t size, int flags)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_getxattr(const char *path, const char *name, char *value,
            size_t size)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_listxattr(const char *path, char *list, size_t size)
{
    return ENOTSUP;
}

int ClusterProxyHelper::sh_removexattr(const char *path, const char *name)
{
    return ENOTSUP;
}

#endif /* HAVE_SETXATTR */

int ClusterProxyHelper::doWrite(std::string path, const std::string &buf, size_t size, off_t offset, ffi_type)
{
    LOG(INFO) << "CluserProxyHelper doWrite(path: " << string(path) << ", size: " << size << ", offset: " << offset << ")";
    
    WriteFile msg;
    msg.set_file_id(path);
    msg.set_data(buf);
    msg.set_offset(offset);

    WriteInfo answer;

    if(!answer.ParseFromString(
        requestMessage(msg.GetDescriptor()->name(), answer.GetDescriptor()->name(), msg.SerializeAsString())))
    {
        LOG(WARNING) << "Cannot parse answer for file: " << string(path);
        return translateError(VEIO);
    }

    LOG(INFO) << "CluserProxyHelper write answer_status: " << answer.answer_status() << ", write real size: " << answer.bytes_written();

    int error = translateError(answer.answer_status());
    if(error == 0) return answer.bytes_written();
    else           return error;
    return 0;
}

int ClusterProxyHelper::doRead(std::string path, std::string &buf, size_t, off_t, ffi_type)
{
    return 0;
}

ClusterProxyHelper::ClusterProxyHelper(std::vector<std::string> args)
  : m_bufferAgent(
        boost::bind(&ClusterProxyHelper::doWrite, this, _1, _2, _3, _4, _5),
        boost::bind(&ClusterProxyHelper::doRead, this, _1, _2, _3, _4, _5))
{
    if(args.size() >= 3) { // If arguments are given, use them to establish connection instead default VeilHelpers configuration
        m_clusterHostname   = args[0];
        m_clusterPort       = utils::fromString<unsigned int>(args[1]);
        m_proxyCert         = args[2];

        m_connectionPool.reset(new SimpleConnectionPool(m_clusterHostname, m_clusterPort, m_proxyCert, NULL));
    } else { // Otherwise init local config using global values
        m_clusterHostname   = config::clusterHostname;
        m_clusterPort       = config::clusterPort;
        m_proxyCert         = config::proxyCert;
    }
}

ClusterProxyHelper::~ClusterProxyHelper()
{
}

} // namespace helpers
} // namespace veil
