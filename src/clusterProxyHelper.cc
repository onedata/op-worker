/**
 * @file clusterProxyHelper.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "clusterProxyHelper.h"

#include "communication/communicator.h"
#include "communication/exception.h"
#include "logging.h"
#include "remote_file_management.pb.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/any.hpp>

#include <functional>

using namespace std;
using namespace std::placeholders;
using namespace one::clproto::remote_file_management;
using namespace one::clproto::communication_protocol;

namespace one {
namespace helpers {

std::unique_ptr<RemoteFileMangement> wrap(const google::protobuf::Message &msg, const std::string spaceId)
{
    auto wrapper = std::make_unique<RemoteFileMangement>();
    wrapper->set_message_type(boost::algorithm::to_lower_copy(msg.GetDescriptor()->name()));
    msg.SerializeToString(wrapper->mutable_input());
    wrapper->set_space_id(spaceId);
    return wrapper;
}

template<typename AnswerType>
string ClusterProxyHelper::requestMessage(const google::protobuf::Message &msg,
                                          const std::chrono::milliseconds timeout)
{
    try
    {
        const auto answer = m_communicator->communicate<AnswerType>(
                    communication::ServerModule::REMOTE_FILES_MANAGER, *wrap(msg, m_spaceId), 2, timeout);
        return answer->worker_answer();
    }
    catch(communication::Exception &e)
    {
        LOG(WARNING) << "Communication error: " << e.what();
    }

    return {};
}

template<typename AnswerType>
string ClusterProxyHelper::requestMessage(const google::protobuf::Message &msg)
{
    try
    {
        const auto answer = m_communicator->communicate<AnswerType>(
                    communication::ServerModule::REMOTE_FILES_MANAGER, *wrap(msg, m_spaceId), 2);
        return answer->worker_answer();
    }
    catch(communication::Exception &e)
    {
        LOG(WARNING) << "Communication error: " << e.what();
    }

    return {};
}

string ClusterProxyHelper::requestAtom(const google::protobuf::Message &msg)
{
    try
    {
        const auto answer = m_communicator->communicate<Atom>(
                    communication::ServerModule::REMOTE_FILES_MANAGER, *wrap(msg, m_spaceId), 2);

        Atom atom;
        if(answer->has_worker_answer())
        {
            atom.ParseFromString(answer->worker_answer());
            return atom.value();
        }
    }
    catch(communication::Exception &e)
    {
        LOG(WARNING) << "Communication error: " << e.what();
    }

    return {};
}

//////////////////////
// Helper callbacks //
//////////////////////

boost::shared_future<struct stat>
ClusterProxyHelper::sh_getattr(const boost::filesystem::path &p)
{
    auto promise = std::make_shared<boost::promise<struct stat>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_access(const boost::filesystem::path &p, int mask)
{
    auto promise = std::make_shared<boost::promise<int>>();
    set_result(promise, 0);

    return promise->get_future();
}


boost::shared_future<std::string>
ClusterProxyHelper::sh_readlink(const boost::filesystem::path &p)
{
    auto promise = std::make_shared<boost::promise<std::string>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<std::vector<std::string>>
ClusterProxyHelper::sh_readdir(const boost::filesystem::path &p, off_t offset, size_t count, StorageHelperCTX &ctx)
{
    auto promise = std::make_shared<boost::promise<std::vector<std::string>>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_mknod(const boost::filesystem::path &p, mode_t mode, dev_t rdev)
{
    auto promise = std::make_shared<boost::promise<int>>();

    m_worker_service.post([&, promise]() {
        DLOG(INFO) << "CluserProxyHelper mknod(path: " << p.string() << ")";

        CreateFile msg;
        msg.set_file_id(p.string());
        msg.set_mode(mode);

        set_result(promise, translateError(requestAtom(msg)));
    });

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_mkdir(const boost::filesystem::path &p, mode_t mode)
{
    auto promise = std::make_shared<boost::promise<int>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_unlink(const boost::filesystem::path &p)
{
    auto promise = std::make_shared<boost::promise<int>>();



    m_worker_service.post([&, promise]() {
        DLOG(INFO) << "CluserProxyHelper unlink(path: " << p.string() << ")";

        DeleteFileAtStorage msg;
        msg.set_file_id(p.string());

        set_result(promise, translateError(requestAtom(msg)));
    });

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_rmdir(const boost::filesystem::path &p)
{
    auto promise = std::make_shared<boost::promise<int>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_symlink(const boost::filesystem::path &from, const boost::filesystem::path &to)
{
    auto promise = std::make_shared<boost::promise<int>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_rename(const boost::filesystem::path &from, const boost::filesystem::path &to)
{
    auto promise = std::make_shared<boost::promise<int>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_link(const boost::filesystem::path &from, const boost::filesystem::path &to)
{
    auto promise = std::make_shared<boost::promise<int>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_chmod(const boost::filesystem::path &p, mode_t mode)
{
    auto promise = std::make_shared<boost::promise<int>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_chown(const boost::filesystem::path &p, uid_t uid, gid_t gid)
{
    auto promise = std::make_shared<boost::promise<int>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_truncate(const boost::filesystem::path &p, off_t size)
{
    auto promise = std::make_shared<boost::promise<int>>();

    m_worker_service.post([&, promise]() {
        DLOG(INFO) << "CluserProxyHelper truncate(path: " << p.string() << ", size: " << size << ")";

        TruncateFile msg;
        msg.set_file_id(p.string());
        msg.set_length(size);

        set_result(promise, translateError(requestAtom(msg)));
    });

    return promise->get_future();
}




boost::shared_future<int>
ClusterProxyHelper::sh_open(const boost::filesystem::path &p, StorageHelperCTX &ctx)
{
    auto promise = std::make_shared<boost::promise<int>>();

    m_worker_service.post([&, promise]() {
        DLOG(INFO) << "CluserProxyHelper open(path: " << p.string() << ")";

        // Proxy this call to Buffer Agent
        set_result(promise, m_bufferAgent.onOpen(p.string(), &ctx.m_ffi));
    });

    return promise->get_future();
}


boost::shared_future<boost::asio::mutable_buffer>
ClusterProxyHelper::sh_read(const boost::filesystem::path &p, boost::asio::mutable_buffer buf, off_t offset,
        StorageHelperCTX &ctx)
{
    auto promise = std::make_shared<boost::promise<boost::asio::mutable_buffer>>();

    m_worker_service.post([&, promise]() {
        // Proxy this call to Buffer Agent
        auto ret = m_bufferAgent.onRead(p.string(), buf, offset, &ctx.m_ffi);
        if(ret < 0) {
            set_posix_error(promise, ret);
        } else {
            auto retBuf = boost::asio::buffer(buf, ret);

            promise->set_value(std::move(retBuf));
        }
    });

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_write(const boost::filesystem::path &p, boost::asio::const_buffer buf, off_t offset,
         StorageHelperCTX &ctx)
{
    auto promise = std::make_shared<boost::promise<int>>();

    m_worker_service.post([&, promise]() {
        DLOG(INFO) << "CluserProxyHelper write(path: " << p.string() << ", size: " << boost::asio::buffer_size(buf) << ", offset: " << offset << ")";

        // Proxy this call to Buffer Agent
        set_result(promise, m_bufferAgent.onWrite(p.string(), buf, offset, &ctx.m_ffi));
    });

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_release(const boost::filesystem::path &p, StorageHelperCTX &ctx)
{
    auto promise = std::make_shared<boost::promise<int>>();

    m_worker_service.post([&, promise]() {
        DLOG(INFO) << "CluserProxyHelper release(path: " << p.string() << ")";

        // Proxy this call to Buffer Agent
        set_result(promise, m_bufferAgent.onRelease(p.string(), &ctx.m_ffi));
    });

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_flush(const boost::filesystem::path &p, StorageHelperCTX &ctx)
{
    auto promise = std::make_shared<boost::promise<int>>();

    m_worker_service.post([&, promise]() {
        DLOG(INFO) << "CluserProxyHelper flush(path: " << p.string() << ")";

        // Proxy this call to Buffer Agent
        set_result(promise, m_bufferAgent.onFlush(p.string(), &ctx.m_ffi));
    });

    return promise->get_future();
}


boost::shared_future<int>
ClusterProxyHelper::sh_fsync(const boost::filesystem::path &p, int isdatasync, StorageHelperCTX &ctx)
{
    auto promise = std::make_shared<boost::promise<int>>();
    set_posix_error(promise, ENOTSUP);

    return promise->get_future();
}


//
//int ClusterProxyHelper::sh_write(const char *path, const char *buf, size_t size,
//             off_t offset, struct fuse_file_info *fi)
//{
//    DLOG(INFO) << "CluserProxyHelper write(path: " << p.string() << ", size: " << size << ", offset: " << offset << ")";
//
//    // Proxy this call to Buffer Agent
//    return m_bufferAgent.onWrite(p.string(), string(buf, size), size, offset, fi);
//}
//

//
int ClusterProxyHelper::doWrite(const boost::filesystem::path &p, boost::asio::const_buffer buf, off_t offset, ffi_type)
{
    LOG(INFO) << "CluserProxyHelper doWrite(path: " << p.string() << ", size: " << boost::asio::buffer_size(buf) << ", offset: " << offset << ")";

    WriteFile msg;
    msg.set_file_id(p.string());
    msg.set_data(boost::asio::buffer_cast<const char*>(buf), boost::asio::buffer_size(buf));
    msg.set_offset(offset);

    WriteInfo answer;
    string inputData = msg.SerializeAsString();

    if(!answer.ParseFromString(requestMessage<WriteInfo>(msg)))
    {
        LOG(WARNING) << "Cannot parse answer for file: " << p.string();
        return translateError(VEIO);
    }

    DLOG(INFO) << "CluserProxyHelper write answer_status: " << answer.answer_status() << ", write real size: " << answer.bytes_written();

    int error = translateError(answer.answer_status());
    if(error == 0) return answer.bytes_written();
    else           return error;
}

int ClusterProxyHelper::doRead(const boost::filesystem::path &p, boost::asio::mutable_buffer buf, off_t offset, ffi_type)
{
    const auto size = boost::asio::buffer_size(buf);

    ReadFile msg;
    msg.set_file_id(p.string());
    msg.set_size(size);
    msg.set_offset(offset);

    FileData answer;
    string inputData = msg.SerializeAsString();

    std::chrono::milliseconds timeout{size * 2}; // 2ms for each byte (minimum of 500B/s);

    if(!answer.ParseFromString(requestMessage<FileData>(msg, timeout)))
    {
        LOG(WARNING) << "Cannot parse answer for file: " << p.string();
        return translateError(VEIO);
    }

    DLOG(INFO) << "CluserProxyHelper(offset: " << offset << ", size: " << size << ") read answer_status: " << answer.answer_status() << ", read real size: " << answer.data().size();

    if(answer.answer_status() == VOK) {
        auto answerBuffer = boost::asio::buffer(answer.data());
        return boost::asio::buffer_copy(buf, answerBuffer);
    } else if(answer.answer_status() == "ok:TODO2") {
        /// TODO: implement big read
        LOG(ERROR) << "Cluster requested to read file (" << p.string() << ") directly over TCP/IP which is not implemented yet";
        return -ENOTSUP;
    } else {
        return translateError(answer.answer_status());
    }
}

ClusterProxyHelper::ClusterProxyHelper(std::shared_ptr<communication::Communicator> communicator,
                                       const BufferLimits &limits, const ArgsMap &args,
                                       boost::asio::io_service &service)
  : m_bufferAgent(
        limits,
        std::bind(&ClusterProxyHelper::doWrite, this, _1, _2, _3, _4),
        std::bind(&ClusterProxyHelper::doRead, this, _1, _2, _3, _4))
  , m_communicator{std::move(communicator)}
  , m_worker_service{service}
{
    m_clusterHostname = args.count("cluster_hostname") ?
                boost::any_cast<std::string>(args.at("cluster_hostname")) : std::string{};

    m_clusterPort = args.count("cluster_port") ?
                boost::any_cast<unsigned int>(args.at("cluster_port")) : 0;

    const auto arg = srvArg(0);
    m_spaceId = args.count(arg)
                    ? boost::any_cast<std::string>(args.at(arg))
                    : string();
}

} // namespace helpers
} // namespace one
