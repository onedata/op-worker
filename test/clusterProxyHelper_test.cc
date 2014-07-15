/**
 * @file clusterProxyHelper_test.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "clusterProxyHelper_proxy.h"
#include "communication/communicator_mock.h"
#include "helpers/storageHelperFactory.h"
#include "make_unique.h"
#include "veilErrors.h"

#include <boost/algorithm/string.hpp>
#include <google/protobuf/descriptor.h>
#include <gtest/gtest.h>

#include "remote_file_management.pb.h"

#include <errno.h>

#include <cstring>

using namespace ::testing;
using namespace veil;
using namespace veil::helpers;
using namespace veil::protocol::remote_file_management;
using namespace veil::protocol::communication_protocol;
using namespace std::placeholders;
using veil::helpers::utils::tolower;

template<typename T>
bool identityEqual(const T &lhs, const T &rhs)
{
    return &lhs == &rhs;
}

class ClusterProxyHelperTest: public ::testing::Test
{
protected:
    std::shared_ptr<MockCommunicator> mockCommunicator;
    std::shared_ptr<ProxyClusterProxyHelper> proxy;

    struct fuse_file_info ffi;
    char buf[1024];

    void SetUp() override
    {
        mockCommunicator = std::make_shared<MockCommunicator>();
        proxy = std::make_shared<ProxyClusterProxyHelper>(mockCommunicator,
                                                          IStorageHelper::ArgsMap{});
    }
};

// TODO:
//TEST_F(ClusterProxyHelperTest, requestMessage)
//{
//    auto answer = std::make_unique<Answer>();

//    answer.set_answer_status("ok");
//    answer.set_worker_answer("worker");
//    EXPECT_CALL(*mockCommunicator, communicate(_, _, _, _)).WillOnce(Return(answer));

//    std::string inputData = "inputData";
//    EXPECT_EQ("worker", proxy->requestMessage("inputType", "answerType", inputData));
//}

//TEST_F(ClusterProxyHelperTest, requestAtom)
//{
//    Answer answer;
//    Atom atom;
//    atom.set_value("value");
//    answer.set_answer_status("ok");
//    answer.set_worker_answer(atom.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(Return(answer));

//    EXPECT_EQ("value", proxy->requestAtom("inputType", "inputData"));
//}

//TEST_F(ClusterProxyHelperTest, open)
//{
//    EXPECT_EQ(0, proxy->sh_open("file_id", &ffi));
//}


//TEST_F(ClusterProxyHelperTest, read)
//{
//    FileData resp;
//    Answer answer;
//    ClusterMsg msg;
//    std::string sbuf;

//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-EIO, proxy->doRead("file_id", sbuf, 10, 2, &ffi));

//    answer.set_answer_status(VOK);

//    resp.set_answer_status(VOK);
//    char str[] = {0, 1, 45, 34, 0, 0, 0, 34, 56};
//    std::string strRaw(str, 9);
//    resp.set_data(strRaw);
//    answer.set_worker_answer(resp.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(9, proxy->doRead("file_id", sbuf, 10, 2, &ffi));
//    for(int i = 0; i < 9; ++i )
//        EXPECT_EQ(str[i], sbuf[i]);

//    resp.set_answer_status(VENOENT);
//    answer.set_worker_answer(resp.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-ENOENT, proxy->doRead("file_id", sbuf, 10, 2, &ffi));

//    RemoteFileMangement rfm;

//    EXPECT_EQ(true, msg.synch());
//    EXPECT_EQ(RFM_MODULE_NAME, msg.module_name());
//    EXPECT_EQ(PROTOCOL_VERSION, msg.protocol_version());
//    EXPECT_EQ(RFM_DECODER, msg.message_decoder_name());
//    EXPECT_EQ(tolower(rfm.GetDescriptor()->name()), msg.message_type());

//    EXPECT_EQ(tolower(FileData::descriptor()->name()), msg.answer_type());
//    EXPECT_EQ(RFM_DECODER, msg.answer_decoder_name());


//    ReadFile subMsg;
//    subMsg.set_file_id("file_id");
//    subMsg.set_size(10);
//    subMsg.set_offset(2);

//    rfm.set_input(subMsg.SerializeAsString());
//    rfm.set_message_type(tolower(subMsg.GetDescriptor()->name()));

//    EXPECT_EQ(rfm.SerializeAsString(), msg.input());
//}


//TEST_F(ClusterProxyHelperTest, write)
//{
//    WriteInfo resp;
//    Answer answer;
//    ClusterMsg msg;
//    char str[] = {0, 1, 45, 34, 0, 0, 0, 34, 56, 2};
//    std::string strRaw(str, 10);
//    std::string sbuf;
//    sbuf = strRaw;

//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-EIO, proxy->doWrite("file_id", sbuf, 10, 2, &ffi));

//    answer.set_answer_status(VOK);

//    resp.set_answer_status(VOK);
//    resp.set_bytes_written(9);
//    answer.set_worker_answer(resp.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(9, proxy->doWrite("file_id", sbuf, 10, 2, &ffi));

//    resp.set_answer_status(VENOENT);
//    answer.set_worker_answer(resp.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-ENOENT, proxy->doWrite("file_id", sbuf, 10, 2, &ffi));

//    RemoteFileMangement rfm;

//    EXPECT_EQ(true, msg.synch());
//    EXPECT_EQ(RFM_MODULE_NAME, msg.module_name());
//    EXPECT_EQ(PROTOCOL_VERSION, msg.protocol_version());
//    EXPECT_EQ(RFM_DECODER, msg.message_decoder_name());
//    EXPECT_EQ(tolower(rfm.GetDescriptor()->name()), msg.message_type());

//    EXPECT_EQ(tolower(WriteInfo::descriptor()->name()), msg.answer_type());
//    EXPECT_EQ(RFM_DECODER, msg.answer_decoder_name());


//    WriteFile subMsg;
//    subMsg.set_file_id("file_id");
//    subMsg.set_data(strRaw);
//    subMsg.set_offset(2);

//    rfm.set_input(subMsg.SerializeAsString());
//    rfm.set_message_type(tolower(subMsg.GetDescriptor()->name()));

//    EXPECT_EQ(rfm.SerializeAsString(), msg.input());
//}


TEST_F(ClusterProxyHelperTest, statfs)
{
    struct statvfs stbuf;
    EXPECT_EQ(ENOTSUP, proxy->sh_statfs("file_id", &stbuf));
}


TEST_F(ClusterProxyHelperTest, release)
{
    EXPECT_EQ(0, proxy->sh_release("file_id", &ffi));
}


TEST_F(ClusterProxyHelperTest, fsync)
{
    EXPECT_EQ(0, proxy->sh_fsync("file_id", 0, &ffi));
}


TEST_F(ClusterProxyHelperTest, getattr)
{
    struct stat stbuf;
    EXPECT_EQ(0, proxy->sh_getattr("file_id", &stbuf));
}


TEST_F(ClusterProxyHelperTest, access)
{
    EXPECT_EQ(0, proxy->sh_access("file_id", 0777));
}


TEST_F(ClusterProxyHelperTest, readlink)
{
    EXPECT_EQ(ENOTSUP, proxy->sh_readlink("file_id", buf, 1024));
}


TEST_F(ClusterProxyHelperTest, readdir)
{
    EXPECT_EQ(ENOTSUP, proxy->sh_readdir("file_id", NULL, NULL, 0, &ffi));
}

//TODO:
//TEST_F(ClusterProxyHelperTest, mknod)
//{
//    Atom atom;
//    Answer answer;
//    ClusterMsg msg;

//    answer.set_answer_status(VOK);
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-EIO, proxy->sh_mknod("file_id", 0755, 0));

//    atom.set_value(VOK);
//    answer.set_worker_answer(atom.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(0, proxy->sh_mknod("file_id", 0755, 0));

//    atom.set_value(VEEXIST);
//    answer.set_worker_answer(atom.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-EEXIST, proxy->sh_mknod("file_id", 0755, 0));

//    RemoteFileMangement rfm;

//    EXPECT_EQ(true, msg.synch());
//    EXPECT_EQ(RFM_MODULE_NAME, msg.module_name());
//    EXPECT_EQ(PROTOCOL_VERSION, msg.protocol_version());
//    EXPECT_EQ(RFM_DECODER, msg.message_decoder_name());
//    EXPECT_EQ(tolower(rfm.GetDescriptor()->name()), msg.message_type());

//    EXPECT_EQ(tolower(Atom::descriptor()->name()), msg.answer_type());
//    EXPECT_EQ(COMMUNICATION_PROTOCOL_DECODER, msg.answer_decoder_name());


//    CreateFile subMsg;
//    subMsg.set_file_id("file_id");

//    rfm.set_input(subMsg.SerializeAsString());
//    rfm.set_message_type(tolower(subMsg.GetDescriptor()->name()));

//    EXPECT_EQ(rfm.SerializeAsString(), msg.input());
//}


//TEST_F(ClusterProxyHelperTest, mkdir)
//{
//    EXPECT_EQ(ENOTSUP, proxy->sh_mkdir("file_id", 0));
//}


//TEST_F(ClusterProxyHelperTest, unlink)
//{
//    Atom atom;
//    Answer answer;
//    ClusterMsg msg;

//    answer.set_answer_status(VOK);
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-EIO, proxy->sh_unlink("file_id"));

//    atom.set_value(VOK);
//    answer.set_worker_answer(atom.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(0, proxy->sh_unlink("file_id"));

//    atom.set_value(VEEXIST);
//    answer.set_worker_answer(atom.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-EEXIST, proxy->sh_unlink("file_id"));

//    RemoteFileMangement rfm;

//    EXPECT_EQ(true, msg.synch());
//    EXPECT_EQ(RFM_MODULE_NAME, msg.module_name());
//    EXPECT_EQ(PROTOCOL_VERSION, msg.protocol_version());
//    EXPECT_EQ(RFM_DECODER, msg.message_decoder_name());
//    EXPECT_EQ(tolower(rfm.GetDescriptor()->name()), msg.message_type());

//    EXPECT_EQ(tolower(Atom::descriptor()->name()), msg.answer_type());
//    EXPECT_EQ(COMMUNICATION_PROTOCOL_DECODER, msg.answer_decoder_name());


//    DeleteFileAtStorage subMsg;
//    subMsg.set_file_id("file_id");

//    rfm.set_input(subMsg.SerializeAsString());
//    rfm.set_message_type(tolower(subMsg.GetDescriptor()->name()));

//    EXPECT_EQ(rfm.SerializeAsString(), msg.input());
//}


TEST_F(ClusterProxyHelperTest, rmdir)
{
    EXPECT_EQ(ENOTSUP, proxy->sh_rmdir("file_id"));
}


TEST_F(ClusterProxyHelperTest, symlink)
{
    EXPECT_EQ(ENOTSUP, proxy->sh_symlink("from", "to"));
}


TEST_F(ClusterProxyHelperTest, rename)
{
    EXPECT_EQ(ENOTSUP, proxy->sh_rename("from", "to"));
}


TEST_F(ClusterProxyHelperTest, link)
{
    EXPECT_EQ(ENOTSUP, proxy->sh_link("from", "to"));
}


TEST_F(ClusterProxyHelperTest, chmod)
{
    EXPECT_EQ(0, proxy->sh_chmod("file_id", 0));
}


TEST_F(ClusterProxyHelperTest, chown)
{
    EXPECT_EQ(0, proxy->sh_chown("file_id", 0, 0));
}

//TODO:
//TEST_F(ClusterProxyHelperTest, truncate)
//{
//    Atom atom;
//    Answer answer;
//    ClusterMsg msg;

//    answer.set_answer_status(VOK);
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-EIO, proxy->sh_truncate("file_id", 10));

//    atom.set_value(VOK);
//    answer.set_worker_answer(atom.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(0, proxy->sh_truncate("file_id", 10));

//    atom.set_value(VEEXIST);
//    answer.set_worker_answer(atom.SerializeAsString());
//    EXPECT_CALL(*mockConnection, communicate(_, _, _)).WillOnce(DoAll(SaveArg<0>(&msg), Return(answer)));
//    EXPECT_EQ(-EEXIST, proxy->sh_truncate("file_id", 10));

//    RemoteFileMangement rfm;

//    EXPECT_EQ(true, msg.synch());
//    EXPECT_EQ(RFM_MODULE_NAME, msg.module_name());
//    EXPECT_EQ(PROTOCOL_VERSION, msg.protocol_version());
//    EXPECT_EQ(RFM_DECODER, msg.message_decoder_name());
//    EXPECT_EQ(tolower(rfm.GetDescriptor()->name()), msg.message_type());

//    EXPECT_EQ(tolower(Atom::descriptor()->name()), msg.answer_type());
//    EXPECT_EQ(COMMUNICATION_PROTOCOL_DECODER, msg.answer_decoder_name());


//    TruncateFile subMsg;
//    subMsg.set_file_id("file_id");
//    subMsg.set_length(10);

//    rfm.set_input(subMsg.SerializeAsString());
//    rfm.set_message_type(tolower(subMsg.GetDescriptor()->name()));

//    EXPECT_EQ(rfm.SerializeAsString(), msg.input());
//}


