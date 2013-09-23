/**
 * @file connectionPool_mock.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef CONNECTION_POOL_MOCK_H
#define CONNECTION_POOL_MOCK_H

#include "simpleConnectionPool.h"
#include "gmock/gmock.h"

using namespace veil; 

class MockConnectionPool
    : public SimpleConnectionPool {
public:
    MockConnectionPool() : SimpleConnectionPool("host", 5555, "certFile", NULL) {};
    ~MockConnectionPool() {};

    MOCK_METHOD2(selectConnection, boost::shared_ptr<CommunicationHandler>(bool, unsigned int));
    MOCK_METHOD1(releaseConnection, void(boost::shared_ptr<CommunicationHandler>));


};

#endif // CONNECTION_POOL_MOCK_H