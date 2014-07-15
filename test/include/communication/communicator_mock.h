/**
 * @file communicator_mock.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATOR_MOCK_H
#define VEILHELPERS_COMMUNICATOR_MOCK_H


#include "communication/communicator.h"

#include <gmock/gmock.h>

class MockCommunicator: public veil::communication::Communicator
{
    using Answer = veil::protocol::communication_protocol::Answer;

public:
    MockCommunicator()
        : veil::communication::Communicator{{}, {}}
    {
    }

    MOCK_METHOD3(communicateAsync, std::future<std::unique_ptr<Answer>>(
        const std::string&, const google::protobuf::Message&,
        const google::protobuf::Message&));

    MOCK_METHOD4(communicate, std::unique_ptr<Answer>(
        const std::string&, const google::protobuf::Message&,
        const google::protobuf::Message&, const std::chrono::milliseconds));

    MOCK_METHOD2(send, void(const std::string&, const google::protobuf::Message&));
};


#endif // VEILHELPERS_COMMUNICATOR_MOCK_H
