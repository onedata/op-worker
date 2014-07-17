/**
 * @file connectionPool_mock.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef CONNECTION_POOL_MOCK_H
#define CONNECTION_POOL_MOCK_H


#include "simpleConnectionPool.h"

#include <gmock/gmock.h>

#include <memory>

class MockConnectionPool: public veil::SimpleConnectionPool
{
public:
    MockConnectionPool()
        : SimpleConnectionPool{"host", 5555, std::bind(&MockConnectionPool::getCertInfo, this)}
    {
    };

    MOCK_METHOD2(setPoolSize, void(PoolType, unsigned int));
    MOCK_METHOD2(setPushCallback, void(const std::string&, veil::push_callback));

    MOCK_METHOD1(selectConnection, std::shared_ptr<veil::CommunicationHandler>(veil::SimpleConnectionPool::PoolType));
    MOCK_METHOD1(releaseConnection, void(std::shared_ptr<veil::CommunicationHandler>));

    veil::CertificateInfo getCertInfo()
    {
        return {"certFile", "certFile"};
    }
};


#endif // CONNECTION_POOL_MOCK_H
