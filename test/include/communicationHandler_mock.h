/**
 * @file communicationHandler_mock.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef COMMUNICAION_HANDLER_MOCK_H
#define COMMUNICAION_HANDLER_MOCK_H

#include "communicationHandler.h"
#include "gmock/gmock.h"

using namespace veil;
using namespace veil::protocol::communication_protocol;

class MockCommunicationHandler
    : public CommunicationHandler {
public:
    MockCommunicationHandler() : CommunicationHandler("host", 5555, boost::bind(&MockCommunicationHandler::getCertInfo, this)) {};
    ~MockCommunicationHandler() {};

    int openConnection() {
        return 0;
    }

    // Override
    void closeConnection() {}

    MOCK_METHOD3(communicate, Answer(ClusterMsg&, uint8_t, uint32_t));
    MOCK_METHOD2(sendMessage, int(ClusterMsg&, int32_t));

    CertificateInfo getCertInfo() {
        return CertificateInfo("certFile", "certFile");
    }
};

#endif // COMMUNICAION_HANDLER_MOCK_H
