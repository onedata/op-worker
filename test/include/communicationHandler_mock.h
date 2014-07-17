/**
 * @file communicationHandler_mock.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef COMMUNICAION_HANDLER_MOCK_H
#define COMMUNICAION_HANDLER_MOCK_H


#include "communicationHandler.h"

#include <gmock/gmock.h>

#include <functional>

class MockCommunicationHandler: public veil::CommunicationHandler
{
public:
    MockCommunicationHandler()
        : CommunicationHandler{"host", 5555, std::bind(&MockCommunicationHandler::getCertInfo, this), {}, false}
    {
    };

    int openConnection() override
    {
        return 0;
    }

    void closeConnection() override
    {
    }

    MOCK_METHOD3(communicate, veil::protocol::communication_protocol::Answer(veil::protocol::communication_protocol::ClusterMsg&, uint8_t, uint32_t));
    MOCK_METHOD2(sendMessage, int(veil::protocol::communication_protocol::ClusterMsg&, int32_t));

    veil::CertificateInfo getCertInfo()
    {
        return {"certFile", "certFile"};
    }
};


#endif // COMMUNICAION_HANDLER_MOCK_H
