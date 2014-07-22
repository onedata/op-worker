/**
 * @file communicationHandler_test.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicationHandler.h"

#include "communication/connection.h"
#include "communication/connectionPool.h"

#include "make_unique.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <thread>

using namespace ::testing;
using namespace std::placeholders;

struct ConnectionPoolMock: public veil::communication::ConnectionPool
{
    ConnectionPoolMock()
        : veil::communication::ConnectionPool{1, "uri"}
    {
        ON_CALL(*this, setOnMessageCallback(_)).WillByDefault(SaveArg<0>(&onMessageCallback));
    }

    std::function<void(const std::string&)> onMessageCallback;

    MOCK_METHOD1(send, void(const std::string&));
    MOCK_METHOD1(setOnMessageCallback, void(std::function<void(const std::string&)>));
    MOCK_METHOD1(addHandshake, void(std::function<std::string()> handshake));
    MOCK_METHOD2(addHandshake, void(std::function<std::string()> handshake,
                                    std::function<std::string()> goodbye));

    MOCK_METHOD0(createConnection, std::unique_ptr<veil::communication::Connection>());
};

struct CommunicationHandlerTest: public ::testing::Test
{
    ConnectionPoolMock *dataPool;
    ConnectionPoolMock *metaPool;
    std::unique_ptr<veil::communication::CommunicationHandler> communicationHandler;

    using Pool = veil::communication::CommunicationHandler::Pool;

    CommunicationHandlerTest()
    {
        auto dataPoolPtr = std::make_unique<ConnectionPoolMock>();
        auto metaPoolPtr = std::make_unique<ConnectionPoolMock>();

        dataPool = dataPoolPtr.get();
        metaPool = metaPoolPtr.get();

        communicationHandler = std::make_unique<veil::communication::CommunicationHandler>(
                    std::move(dataPoolPtr), std::move(metaPoolPtr));
    }
};

veil::protocol::communication_protocol::ClusterMsg randomMessage()
{
    std::random_device rd;
    std::mt19937 gen{rd()};

    auto randomInt = [&]{ return std::uniform_int_distribution<>{1, 50}(gen); };
    auto randomString = [&]{
        std::uniform_int_distribution<char> dis{'a', 'z'};
        std::string str;
        std::generate_n(std::back_inserter(str), randomInt(), [&]{ return dis(gen); });
        return std::move(str);
    };

    veil::protocol::communication_protocol::ClusterMsg message;
    message.set_answer_decoder_name(randomString());
    message.set_answer_type(randomString());
    message.set_input(randomString());
    message.set_message_decoder_name(randomString());
    message.set_message_type(randomString());
    message.set_module_name(randomString());
    message.set_protocol_version(randomInt());
    message.set_synch(true);

    return message;
}

CommunicationHandlerTest::Pool randomPool()
{
    std::random_device rd;
    std::mt19937 gen{rd()};

    return std::bernoulli_distribution{0.5}(gen)
        ? CommunicationHandlerTest::Pool::META
        : CommunicationHandlerTest::Pool::DATA;
}

TEST_F(CommunicationHandlerTest, shouldSetOnMessageCallbackOnPools)
{
    auto dataPool = std::make_unique<ConnectionPoolMock>();
    auto metaPool = std::make_unique<ConnectionPoolMock>();

    EXPECT_CALL(*dataPool, setOnMessageCallback(_));
    EXPECT_CALL(*metaPool, setOnMessageCallback(_));

    std::make_unique<veil::communication::CommunicationHandler>(
                std::move(dataPool), std::move(metaPool));
}

TEST_F(CommunicationHandlerTest, shouldCallSendOnAppropriatePoolOnSend)
{
    auto dataMsg = randomMessage();
    auto metaMsg = randomMessage();

    std::string sentDataMessage;
    std::string sentMetaMessage;

    EXPECT_CALL(*dataPool, send(_)).WillOnce(SaveArg<0>(&sentDataMessage));
    EXPECT_CALL(*metaPool, send(_)).WillOnce(SaveArg<0>(&sentMetaMessage));

    communicationHandler->send(dataMsg, Pool::DATA);
    communicationHandler->send(metaMsg, Pool::META);

    ASSERT_EQ(dataMsg.SerializeAsString(), sentDataMessage);
    ASSERT_EQ(metaMsg.SerializeAsString(), sentMetaMessage);
}

TEST_F(CommunicationHandlerTest, shouldSetMessageIdOnSend)
{
    auto dataMsg = randomMessage();

    ASSERT_FALSE(dataMsg.has_message_id());
    communicationHandler->send(dataMsg, randomPool());
    ASSERT_TRUE(dataMsg.has_message_id());
}

TEST_F(CommunicationHandlerTest, shouldSetIncrementingMessageIds)
{
    auto dataMsg = randomMessage();

    communicationHandler->send(dataMsg, randomPool());
    auto msgId = dataMsg.message_id();

    communicationHandler->send(dataMsg, randomPool());
    ASSERT_GT(dataMsg.message_id(), msgId);
}

TEST_F(CommunicationHandlerTest, shouldCallSendOnAppropriatePoolOnCommunicate)
{
    auto dataMsg = randomMessage();
    auto metaMsg = randomMessage();

    std::string sentDataMessage;
    std::string sentMetaMessage;

    EXPECT_CALL(*dataPool, send(_)).WillOnce(SaveArg<0>(&sentDataMessage));
    EXPECT_CALL(*metaPool, send(_)).WillOnce(SaveArg<0>(&sentMetaMessage));

    communicationHandler->communicate(dataMsg, Pool::DATA);
    communicationHandler->communicate(metaMsg, Pool::META);

    ASSERT_EQ(dataMsg.SerializeAsString(), sentDataMessage);
    ASSERT_EQ(metaMsg.SerializeAsString(), sentMetaMessage);
}

TEST_F(CommunicationHandlerTest, shouldFulfilAPromiseOnResultMessage)
{
    auto poolType = randomPool();
    auto pool = poolType == Pool::META ? metaPool : dataPool;
    auto dataMsg = randomMessage();

    auto future = communicationHandler->communicate(dataMsg, randomPool());
    auto messageId = dataMsg.message_id();

    veil::protocol::communication_protocol::Answer answer;
    answer.set_answer_status("answer status");
    answer.set_message_id(messageId);

    std::thread t{[&]{ pool->onMessageCallback(answer.SerializeAsString()); }};

    auto status = future.wait_for(std::chrono::seconds{5});
    ASSERT_EQ(std::future_status::ready, status);
    EXPECT_EQ(answer.SerializeAsString(), future.get()->SerializeAsString());

    t.join();
}

TEST_F(CommunicationHandlerTest, shouldCallSubscribedCallbackOnPredicateFulfilment)
{
    auto pred = [](const veil::protocol::communication_protocol::Answer &ans) {
        return ans.message_id() == 128 || ans.message_id() == 256 || ans.message_id() == 512;
    };

    int callbackCalled = 0;
    veil::protocol::communication_protocol::Answer answerGiven;
    auto callback = [&](const veil::protocol::communication_protocol::Answer &ans) {
        ++callbackCalled;
        answerGiven = ans;
        return true;
    };

    communicationHandler->subscribe(
                veil::communication::CommunicationHandler::SubscriptionData{pred, callback});

    for(int i = 0; i < 1000; ++i)
    {
        veil::protocol::communication_protocol::Answer answer;
        answer.set_answer_status("answer status");
        answer.set_message_id(i);

        auto pool = randomPool() == CommunicationHandlerTest::Pool::META ? metaPool : dataPool;
        pool->onMessageCallback(answer.SerializeAsString());
    }

    EXPECT_EQ(3, callbackCalled);
    EXPECT_EQ(512, answerGiven.message_id());
}

TEST_F(CommunicationHandlerTest, shouldUnsubscribeWhenCallbackReturnsFalse)
{
    int predicateCalled = 0;
    auto pred = [&](const veil::protocol::communication_protocol::Answer &ans) {
        ++predicateCalled;
        return true;
    };

    int callbackCalled = 0;
    auto callback = [&](const veil::protocol::communication_protocol::Answer &ans) {
        return ++callbackCalled < 100;
    };

    communicationHandler->subscribe(
                veil::communication::CommunicationHandler::SubscriptionData{pred, callback});

    for(int i = 0; i < 1000; ++i)
    {
        veil::protocol::communication_protocol::Answer answer;
        answer.set_answer_status("answer status");
        answer.set_message_id(i);

        auto pool = randomPool() == CommunicationHandlerTest::Pool::META ? metaPool : dataPool;
        pool->onMessageCallback(answer.SerializeAsString());
    }

    EXPECT_EQ(100, callbackCalled);
    EXPECT_EQ(100, predicateCalled);
}

TEST_F(CommunicationHandlerTest, shouldPassHandshakeAndGoodbyeToDataPool)
{
    EXPECT_CALL(*dataPool, addHandshake(_, _));
    communicationHandler->addHandshake(randomMessage(), randomMessage(), Pool::DATA);
}

TEST_F(CommunicationHandlerTest, shouldPassHandshakeAndGoodbyeToMetaPool)
{
    EXPECT_CALL(*metaPool, addHandshake(_, _));
    communicationHandler->addHandshake(randomMessage(), randomMessage(), Pool::META);
}

TEST_F(CommunicationHandlerTest, shouldPassHandshakeToDataPool)
{
    EXPECT_CALL(*dataPool, addHandshake(_));
    communicationHandler->addHandshake(randomMessage(), Pool::DATA);
}

TEST_F(CommunicationHandlerTest, shouldPassHandshakeToMetaPool)
{
    EXPECT_CALL(*metaPool, addHandshake(_));
    communicationHandler->addHandshake(randomMessage(), Pool::META);
}

void checkMessageGenerator(const std::function<std::string()> &gen,
                           veil::protocol::communication_protocol::ClusterMsg original)
{
    ASSERT_FALSE(original.has_message_id());

    decltype(original) generated1;
    generated1.ParseFromString(gen());
    ASSERT_TRUE(generated1.has_message_id());

    decltype(original) generated2;
    generated2.ParseFromString(gen());
    ASSERT_TRUE(generated2.has_message_id());

    ASSERT_GE(generated2.message_id(), generated1.message_id());

    original.set_message_id(generated1.message_id());
    ASSERT_EQ(original.SerializeAsString(), generated1.SerializeAsString());

    original.set_message_id(generated2.message_id());
    ASSERT_EQ(original.SerializeAsString(), generated2.SerializeAsString());
}

TEST_F(CommunicationHandlerTest, shouldGenerateIdsForHandshakeAndGoodbyeMessages)
{
    auto poolType = randomPool();
    auto pool = poolType == Pool::META ? metaPool : dataPool;
    auto originalHandshake = randomMessage();
    auto originalGoodbye = randomMessage();

    std::function<std::string()> handshakeGenerator;
    std::function<std::string()> goodbyeGenerator;

    EXPECT_CALL(*pool, addHandshake(_, _)).WillOnce(
                DoAll(SaveArg<0>(&handshakeGenerator),
                      SaveArg<1>(&goodbyeGenerator)));

    communicationHandler->addHandshake(originalHandshake, originalGoodbye, poolType);

    checkMessageGenerator(handshakeGenerator, originalHandshake);
    checkMessageGenerator(goodbyeGenerator, originalGoodbye);
}


TEST_F(CommunicationHandlerTest, shouldGenerateIdsForHandshakeMessages)
{
    auto poolType = randomPool();
    auto pool = poolType == Pool::META ? metaPool : dataPool;
    auto originalHandshake = randomMessage();

    std::function<std::string()> handshakeGenerator;

    EXPECT_CALL(*pool, addHandshake(_)).WillOnce(SaveArg<0>(&handshakeGenerator));

    communicationHandler->addHandshake(originalHandshake, poolType);

    checkMessageGenerator(handshakeGenerator, originalHandshake);
}
