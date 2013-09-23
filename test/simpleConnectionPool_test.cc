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
    shared_ptr<ProxySimpleConnectionPool> proxy;

    virtual void SetUp() {
        proxy.reset(new ProxySimpleConnectionPool("host", 5555, "cert", NULL));
    }

    virtual void TearDown() {
    }

};

// Test selectConnection method
TEST_F(SimpleConnectionPoolTest, selectAndReleaseConnection)
{
    shared_ptr<MockCommunicationHandler> conn1(new MockCommunicationHandler());
    shared_ptr<MockCommunicationHandler> conn2(new MockCommunicationHandler());
    shared_ptr<MockCommunicationHandler> conn3(new MockCommunicationHandler());

    proxy->addConnection(conn1);
    proxy->addConnection(conn2);
    proxy->addConnection(conn3);

    EXPECT_EQ(conn1, proxy->selectConnection());
    EXPECT_EQ(conn2, proxy->selectConnection());

    proxy->releaseConnection(conn2);

    EXPECT_EQ(shared_ptr<MockCommunicationHandler>(), proxy->selectConnection(true));

    EXPECT_EQ(conn2, proxy->selectConnection());
    EXPECT_EQ(conn3, proxy->selectConnection());

    proxy->releaseConnection(conn1);

    EXPECT_EQ(conn1, proxy->selectConnection());

    // New connection (null since CommunicationHandler fails)
    EXPECT_EQ(shared_ptr<MockCommunicationHandler>(), proxy->selectConnection());
}
