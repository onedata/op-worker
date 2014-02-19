/**
 * @file simpleConnectionPool_test.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "testCommonH.h"
#include "connectionPool_mock.h"
#include "communicationHandler_mock.h"
#include "simpleConnectionPool_proxy.h"

using namespace boost;

INIT_AND_RUN_ALL_TESTS(); // TEST RUNNER !

// TEST definitions below

class SimpleConnectionPoolTest
    : public ::testing::Test {

protected:
    MockConnectionPool mockPool;
    boost::shared_ptr<ProxySimpleConnectionPool> proxy;

    virtual void SetUp() {
        proxy.reset(new ProxySimpleConnectionPool("host", 5555, boost::bind(&SimpleConnectionPoolTest::getCertInfo, this)));
    }

    virtual void TearDown() {
    }

    CertificateInfo getCertInfo() {
        return CertificateInfo("certFile", "certFile");
    }

};

// Test selectConnection method
TEST_F(SimpleConnectionPoolTest, poolHasFixedSize_RoundRobinWorks)
{
    boost::shared_ptr<CommunicationHandler> connM[6], connD[4];

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
