/**
 * @file simpleConnectionPool_test.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communicationHandler_mock.h"
#include "connectionPool_mock.h"
#include "simpleConnectionPool_proxy.h"

#include <gtest/gtest.h>

using namespace ::testing;
using namespace veil;

class SimpleConnectionPoolTest: public ::testing::Test
{
protected:
    MockConnectionPool mockPool;
    std::shared_ptr<ProxySimpleConnectionPool> proxy;

    void SetUp() override
    {
        proxy = std::make_shared<ProxySimpleConnectionPool>("host", 5555, std::bind(&SimpleConnectionPoolTest::getCertInfo, this));
    }

    CertificateInfo getCertInfo()
    {
        return {"certFile", "certFile"};
    }
};

// Test selectConnection method
TEST_F(SimpleConnectionPoolTest, poolHasFixedSize_RoundRobinWorks)
{
    std::shared_ptr<CommunicationHandler> connM[6], connD[4];

    // Set pool sizes
    proxy->setPoolSize(SimpleConnectionPool::META_POOL, 3);
    proxy->setPoolSize(SimpleConnectionPool::DATA_POOL, 2);
    sleep(1); // Give pooler some time to initialize fake connections

    for(int i = 0; i < 4; ++i)
        connM[i] = proxy->selectConnection(SimpleConnectionPool::META_POOL);

    for(int i = 0; i < 4; ++i)
        connD[i] = proxy->selectConnection(SimpleConnectionPool::DATA_POOL);

    for(int i = 4; i < 6; ++i)
        connM[i] = proxy->selectConnection(SimpleConnectionPool::META_POOL);

    // RoundRobin on meta connections
    EXPECT_EQ(connM[0], connM[3]);
    EXPECT_EQ(connM[1], connM[4]);
    EXPECT_EQ(connM[2], connM[5]);

    // RoundRobin on data connections
    EXPECT_EQ(connD[0], connD[2]);
    EXPECT_EQ(connD[1], connD[3]);

    // Returned connections has to be diffenet
    for(int i = 0; i < 6; ++i)
    {
        for(int j = 0; j < 4; ++j)
            EXPECT_NE(connM[i], connD[j]);
        for(int j = 0; j < 3; ++j)
        {
            if(j == i || i == j+3)
                continue;

            EXPECT_NE(connM[i], connM[j]) << "commM[i: " << i << "], commM[j: " << j << "]";
        }
    }
}
