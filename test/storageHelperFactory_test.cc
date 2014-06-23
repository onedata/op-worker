/**
 * @file storageHelperFactory_test.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "testCommonH.h"
#include "helpers/storageHelperFactory.h"

#include "connectionPool_mock.h"

#include <memory>

INIT_AND_RUN_ALL_TESTS(); // TEST RUNNER !

// TEST definitions below


TEST(StorageHelperFactoryTest, ObjectBuild) {
    StorageHelperFactory factory(std::make_shared<MockConnectionPool>(), BufferLimits{});

    EXPECT_NE((IStorageHelper*)0, factory.getStorageHelper("DirectIO", IStorageHelper::ArgsMap{}).get());

    EXPECT_EQ((IStorageHelper*)0, factory.getStorageHelper("not existing", IStorageHelper::ArgsMap{}).get());
}
