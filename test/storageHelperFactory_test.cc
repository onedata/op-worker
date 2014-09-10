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

using namespace veil;
using namespace veil::helpers;

TEST(StorageHelperFactoryTest, ObjectBuild)
{
    StorageHelperFactory factory(std::make_shared<MockCommunicator>(), BufferLimits{});

    EXPECT_NE((IStorageHelper*)0, factory.getStorageHelper("DirectIO", IStorageHelper::ArgsMap{}).get());

    EXPECT_EQ((IStorageHelper*)0, factory.getStorageHelper("not existing", IStorageHelper::ArgsMap{}).get());
}
