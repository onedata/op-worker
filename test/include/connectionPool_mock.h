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
    MockConnectionPool() : SimpleConnectionPool("host", 5555, std::bind(&MockConnectionPool::getCertInfo, this)) {};
    ~MockConnectionPool() {};

    MOCK_METHOD2(setPoolSize, void(PoolType, unsigned int));
    MOCK_METHOD2(setPushCallback, void(const std::string&, push_callback));

    MOCK_METHOD1(selectConnection, std::shared_ptr<CommunicationHandler>(SimpleConnectionPool::PoolType));
    MOCK_METHOD1(releaseConnection, void(std::shared_ptr<CommunicationHandler>));

    CertificateInfo getCertInfo() {
        return CertificateInfo("certFile", "certFile");
    }

};

#endif // CONNECTION_POOL_MOCK_H
