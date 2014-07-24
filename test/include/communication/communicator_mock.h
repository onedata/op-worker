/**
 * @file communicator_mock.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATOR_MOCK_H
#define VEILHELPERS_COMMUNICATOR_MOCK_H


#include "communication/communicator.h"

#include "communication/communicationHandler_mock.h"
#include "make_unique.h"

#include <gmock/gmock.h>

class MockCommunicator: public veil::communication::Communicator
{
    using Answer = veil::protocol::communication_protocol::Answer;
    using Atom = veil::protocol::communication_protocol::Atom;

public:
    MockCommunicator()
        : veil::communication::Communicator{std::make_unique<::testing::NiceMock<MockCommunicationHandler>>()}
    {
    }

    std::future<std::unique_ptr<Answer>> communicateAsync(const std::string &module,
                                                          const google::protobuf::Message &msg,
                                                          const google::protobuf::Message &ans,
                                                          const unsigned int retries) override
    {
        auto value = communicateAsyncMock(module, msg, ans);
        std::promise<std::unique_ptr<Answer>> promise;
        promise.set_value(std::make_unique<Answer>(value));
        return promise.get_future();
    }

    std::unique_ptr<Answer> communicate(const std::string &module,
                                        const google::protobuf::Message &msg,
                                        const google::protobuf::Message &ans,
                                        const unsigned int retries,
                                        const std::chrono::milliseconds timeout) override
    {
        auto value = communicateMock(module, msg, ans, timeout);
        return std::make_unique<Answer>(value);
    }

    MOCK_METHOD3(send, void(const std::string&, const google::protobuf::Message&,
                            const unsigned int));

    MOCK_METHOD3(communicateAsyncMock, Answer(const std::string&,
                                              const google::protobuf::Message&,
                                              const google::protobuf::Message&));

    MOCK_METHOD4(communicateMock, Answer(const std::string&,
                                         const google::protobuf::Message&,
                                         const google::protobuf::Message&,
                                         const std::chrono::milliseconds));
};


#endif // VEILHELPERS_COMMUNICATOR_MOCK_H
