/**
 * @file storageHelperFactory_test.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "helpers/storageHelperFactory.h"

#include "communication/communicator_mock.h"

#include <gtest/gtest.h>

#include <memory>

using namespace one;
using namespace one::helpers;

TEST(StorageHelperFactoryTest, ObjectBuild)
{
    boost::asio::io_service dio_service;
    boost::asio::io_service cproxy_service;

    StorageHelperFactory factory(std::make_shared<MockCommunicator>(), BufferLimits{}, dio_service, cproxy_service);

    EXPECT_NE((IStorageHelper*)nullptr, factory.getStorageHelper("DirectIO", IStorageHelper::ArgsMap{}).get());

    EXPECT_EQ((IStorageHelper*)nullptr, factory.getStorageHelper("not existing", IStorageHelper::ArgsMap{}).get());
}
