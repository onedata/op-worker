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
    MockCommunicationHandler() : CommunicationHandler("host", 5555, "certFile") {};
    ~MockCommunicationHandler() {};

    int openConnection() {
        return 0;
    }

    void initSSL() {
    }

    MOCK_METHOD2(communicate, Answer(ClusterMsg&, uint8_t));
};

#endif // COMMUNICAION_HANDLER_MOCK_H
