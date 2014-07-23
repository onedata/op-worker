/**
 * @file connectionPool_mock.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATION_CONNECTION_POOL_MOCK_H
#define VEILHELPERS_COMMUNICATION_CONNECTION_POOL_MOCK_H


#include "communication/communicator.h"

#include <gmock/gmock.h>

#include <functional>
#include <string>

struct ConnectionPoolMock: public veil::communication::ConnectionPool
{
    ConnectionPoolMock()
        : veil::communication::ConnectionPool{1, "uri"}
    {
        ON_CALL(*this, setOnMessageCallback(::testing::_)).
                WillByDefault(::testing::SaveArg<0>(&onMessageCallback));
    }

    std::function<void(const std::string&)> onMessageCallback;

    MOCK_METHOD1(send, void(const std::string&));
    MOCK_METHOD1(setOnMessageCallback, void(std::function<void(const std::string&)>));
    MOCK_METHOD1(addHandshake, void(std::function<std::string()> handshake));
    MOCK_METHOD2(addHandshake, void(std::function<std::string()> handshake,
                                    std::function<std::string()> goodbye));

    MOCK_METHOD0(createConnection, std::unique_ptr<veil::communication::Connection>());
};


#endif // VEILHELPERS_COMMUNICATION_CONNECTION_POOL_MOCK_H
