/**
 * @file clusterProxyHelper.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef HELPERS_CLUSTER_PROXY_HELPER_H
#define HELPERS_CLUSTER_PROXY_HELPER_H


#include "helpers/IStorageHelper.h"

#include "bufferAgent.h"
#include "communication_protocol.pb.h"

#include <google/protobuf/message.h>

#include <fuse.h>
#include <sys/types.h>

#include <string>
#include <memory>

#include <boost/asio/io_service.hpp>


namespace one
{

namespace communication{ class Communicator; }

namespace helpers
{

struct BufferLimits;

/**
 * The ClusterProxyHelper class
 * Storage helper used to access files through oneprovider (accessed over TLS protocol).
 */
class ClusterProxyHelper: public IStorageHelper
{
public:
    /**
     * This storage helper uses either 0 or 3 arguments. If no arguments are passed, default helpers connection pooling will be used.
     * Otherwise first argument shall be cluster's hostname, second - cluster's port and third one - path to peer certificate.
     */
    ClusterProxyHelper(std::shared_ptr<communication::Communicator>,
                       const BufferLimits &limits, const ArgsMap &args, boost::asio::io_service &service);
    virtual ~ClusterProxyHelper() = default;

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

protected:
    unsigned int      m_clusterPort;
    std::string       m_proxyCert;
    std::string       m_clusterHostname;
    BufferAgent       m_bufferAgent;
    std::string       m_spaceId;

    template<typename AnswerType>
    std::string requestMessage(const google::protobuf::Message &msg,
                               std::chrono::milliseconds timeout);                   ///< Creates & sends ClusterMsg with given types and input. Response is an serialized message od type "answerType".
    template<typename AnswerType>
    std::string requestMessage(const google::protobuf::Message &msg); ///< Creates & sends ClusterMsg with given types and input. Response is an serialized message od type "answerType".
    virtual std::string requestAtom(const google::protobuf::Message &msg);                                              ///< Same as requestMessage except it always receives Atom. Return value is an strign value of Atom.

    virtual int doWrite(const boost::filesystem::path &p, boost::asio::const_buffer buf, off_t, ffi_type);             ///< Real implementation of write operation.
    virtual int doRead(const boost::filesystem::path &p, boost::asio::mutable_buffer buf, off_t, ffi_type);                    ///< Real implementation of read operation.

private:
    const std::shared_ptr<communication::Communicator> m_communicator;
    boost::asio::io_service &m_worker_service;
};

} // namespace helpers
} // namespace one


#endif // HELPERS_CLUSTER_PROXY_HELPER_H
