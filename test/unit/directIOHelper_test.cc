/**
 * @file clusterProxyHelper_test.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "directIOHelper.h"
#include "helpers/storageHelperFactory.h"

#include <boost/algorithm/string.hpp>
#include <gtest/gtest.h>

#include <errno.h>

#include <cstring>
#include <future>
#include <iostream>
#include <fstream>
#include <cstdio>

using namespace ::testing;
using namespace one;
using namespace one::helpers;
using namespace std::placeholders;
using one::helpers::utils::tolower;

template<typename T>
bool identityEqual(const T &lhs, const T &rhs)
{
    return &lhs == &rhs;
}

#define EXPECT_THROW_POSIX_CODE(WHAT, CODE) \
    try { \
        WHAT; \
        FAIL() << "Method should've thrown"; \
    } catch(std::system_error &e) { \
        if(e.code().value() != CODE) { FAIL() << "Invalid error code. Was " << e.code().value() << " (" << e.what() << ") but expected " << CODE; } \
    } catch(...) { FAIL() << "Unknown exception"; }

#define DIO_TEST_ROOT "/tmp"

class DirectIOHelperTest: public ::testing::Test
{
protected:
    std::shared_ptr<DirectIOHelper> proxy;

    struct fuse_file_info ffi = {0};
    StorageHelperCTX ctx;
    char buf[1024];

    boost::asio::io_service io_service;
    boost::asio::io_service::work io_work;
    std::thread th_handle;

    boost::filesystem::path testFilePath;
    boost::filesystem::path testFileId;

    DirectIOHelperTest()
      : ctx(ffi)
      , io_work(io_service)
    {
    }

    void unlinkOnDIO(boost::filesystem::path p)
    {
        std::remove((boost::filesystem::path(DIO_TEST_ROOT) / p).c_str());
    }

    void SetUp() override
    {
        testFileId = "test.txt";
        testFilePath = boost::filesystem::path(DIO_TEST_ROOT) / testFileId;

        th_handle = std::thread([&]() { io_service.run(); });
        proxy = std::make_shared<DirectIOHelper>(IStorageHelper::ArgsMap{{srvArg(0), std::string(DIO_TEST_ROOT)}},
                                                          io_service);

        // remove all files that are used in tests
        unlinkOnDIO("to");
        unlinkOnDIO("dir");
        unlinkOnDIO(testFileId);

        // create test file
        std::ofstream f (testFilePath.string());
        f << "test_123456789_test" << std::endl;
        f.close();
    }

    void TearDown() override
    {
        unlinkOnDIO(testFileId);

        io_service.stop();
        th_handle.join();
    }
};


TEST_F(DirectIOHelperTest, writeAndRead)
{
    std::string stmp("000");
    char ctmp[5];
    auto writeBuf = boost::asio::buffer(stmp);
    auto readBuf = boost::asio::buffer(ctmp);

    auto p1 = proxy->sh_read(testFileId, readBuf, 5, ctx);
    auto rbuf1 = p1.get();
    EXPECT_EQ(5, boost::asio::buffer_size(rbuf1));
    EXPECT_EQ("12345", std::string(boost::asio::buffer_cast<const char*>(rbuf1), boost::asio::buffer_size(rbuf1)));

    auto p2 = proxy->sh_write(testFileId, writeBuf, 5, ctx);
    auto bytes_written = p2.get();
    EXPECT_EQ(3, bytes_written);

    auto p3 = proxy->sh_read(testFileId, readBuf, 5, ctx);
    auto rbuf3 = p3.get();
    EXPECT_EQ(5, boost::asio::buffer_size(rbuf3));
    EXPECT_EQ("00045", std::string(boost::asio::buffer_cast<const char*>(rbuf3), boost::asio::buffer_size(rbuf3)));
}


TEST_F(DirectIOHelperTest, openAndRelease)
{
    auto f1 = proxy->sh_open(testFileId, ctx);
    EXPECT_GT(f1.get(), 0);
    EXPECT_GT(ctx.m_ffi.fh, 0);

    auto p2 = proxy->sh_release(testFileId, ctx);
    EXPECT_EQ(0, p2.get());
    EXPECT_EQ(0, ctx.m_ffi.fh);
}


TEST_F(DirectIOHelperTest, fsync)
{
    auto p = proxy->sh_fsync(testFileId, 0, ctx);
    EXPECT_EQ(0, p.get());
}


TEST_F(DirectIOHelperTest, getattr)
{
    auto p = proxy->sh_getattr(testFileId);
    struct stat stbuf = p.get();
    EXPECT_EQ(20, stbuf.st_size);
}


TEST_F(DirectIOHelperTest, access)
{
    auto p = proxy->sh_access(testFileId, 0);
    EXPECT_EQ(0, p.get());
}


TEST_F(DirectIOHelperTest, readdir)
{
    auto p = proxy->sh_readdir(testFileId, 0, 10, ctx);
    EXPECT_THROW_POSIX_CODE(p.get(), ENOTSUP);
}


TEST_F(DirectIOHelperTest, mknod)
{
    auto p = proxy->sh_mknod(testFileId, S_IFREG, 0);
    EXPECT_THROW_POSIX_CODE(p.get(), EEXIST);
}


TEST_F(DirectIOHelperTest, mkdir)
{
    auto p = proxy->sh_mkdir("dir", 0);
    EXPECT_EQ(0, p.get());

    std::remove("dir");
}


TEST_F(DirectIOHelperTest, unlink)
{
    auto p = proxy->sh_unlink(testFileId);
    EXPECT_EQ(0, p.get());
}


TEST_F(DirectIOHelperTest, rmdir)
{
    auto p = proxy->sh_rmdir(testFileId);
    EXPECT_THROW_POSIX_CODE(p.get(), ENOTDIR);
}


TEST_F(DirectIOHelperTest, symlinkAndReadlink)
{
    auto p1 = proxy->sh_symlink("/from", "to");
    EXPECT_EQ(0, p1.get());

    auto p2 = proxy->sh_readlink("to");
    EXPECT_EQ(std::string(DIO_TEST_ROOT) + "/from", p2.get());

    unlinkOnDIO("to");
}


TEST_F(DirectIOHelperTest, rename)
{
    auto p = proxy->sh_rename(testFileId, "to");
    EXPECT_EQ(0, p.get());

    unlinkOnDIO("to");
}


TEST_F(DirectIOHelperTest, link)
{
    auto p = proxy->sh_link(testFileId, "to");
    EXPECT_EQ(0, p.get());

    unlinkOnDIO("to");
}


TEST_F(DirectIOHelperTest, chmod)
{
    auto p = proxy->sh_chmod(testFileId, 600);
    EXPECT_EQ(0, p.get());
}


TEST_F(DirectIOHelperTest, chown)
{
    auto p = proxy->sh_chown(testFileId, -1, -1);
    EXPECT_EQ(0, p.get());
}


TEST_F(DirectIOHelperTest, truncate)
{

}


