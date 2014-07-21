/**
 * @file connectionPool_test.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/connection.h"
#include "communication/connectionPool.h"

#include "make_unique.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <future>

using namespace ::testing;
using namespace std::placeholders;

struct ConnectionMock: public veil::communication::Connection
{
    static std::atomic<unsigned int> openConnections;

    ConnectionMock(std::function<void(const std::string&)> onMessageCallback,
                   std::function<void(Connection&)> onFailCallback,
                   std::function<void(Connection&)> onOpenCallback,
                   std::function<void(Connection&)> onErrorCallback)
        : veil::communication::Connection{onMessageCallback, onFailCallback,
                                          onOpenCallback, onErrorCallback}
    {
        ++openConnections;
    }

    ~ConnectionMock()
    {
        --openConnections;
    }

    using veil::communication::Connection::Connection;
    using veil::communication::Connection::m_onErrorCallback;
    using veil::communication::Connection::m_onFailCallback;
    using veil::communication::Connection::m_onMessageCallback;
    using veil::communication::Connection::m_onOpenCallback;

    MOCK_METHOD1(send, void(const std::string&));

private:
    std::thread thread;
};
decltype(ConnectionMock::openConnections) ConnectionMock::openConnections{0};

struct ConnectionPoolProxy: public veil::communication::ConnectionPool
{
    using veil::communication::ConnectionPool::ConnectionPool;
    using veil::communication::ConnectionPool::addConnections;
    using veil::communication::ConnectionPool::onFail;
    using veil::communication::ConnectionPool::onOpen;
    using veil::communication::ConnectionPool::onError;
    using veil::communication::ConnectionPool::m_onMessageCallback;

    std::vector<ConnectionMock*> createdConnections;

    MOCK_METHOD0(createConnection, std::unique_ptr<veil::communication::Connection>());
    std::unique_ptr<veil::communication::Connection> createConnection_impl()
    {
        auto c = new ConnectionMock{
            m_onMessageCallback,
            std::bind(&ConnectionPoolProxy::onFail, this, _1),
            std::bind(&ConnectionPoolProxy::onOpen, this, _1),
            std::bind(&ConnectionPoolProxy::onError, this, _1)};

        createdConnections.emplace_back(c);
        return std::unique_ptr<veil::communication::Connection>{c};
    }
};

struct ConnectionPoolTest: public ::testing::Test
{
    std::unique_ptr<ConnectionPoolProxy> connectionPool;
    std::string uri;
    unsigned int connectionNumber;

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<unsigned int> intDis{1, 15};
    std::uniform_int_distribution<char> charDis{'a', 'z'};

    MOCK_METHOD1(onMessage, void(const std::string&));

    ConnectionPoolTest()
    {
        std::generate_n(std::back_inserter(uri), intDis(gen), [&]{return charDis(gen);});
        connectionNumber = intDis(gen);

        connectionPool = std::make_unique<ConnectionPoolProxy>(connectionNumber, uri);
        ON_CALL(*connectionPool, createConnection()).WillByDefault(
                    Invoke(connectionPool.get(), &ConnectionPoolProxy::createConnection_impl));

        connectionPool->setOnMessageCallback(std::bind(&ConnectionPoolTest::onMessage, this, _1));
    }

    void initConnections()
    {
        connectionPool->addConnections();
        for(auto &c: connectionPool->createdConnections)
            c->m_onOpenCallback();
    }
};

TEST_F(ConnectionPoolTest, shouldCreateExpectedNumberOfConnectionsOnAddConnections)
{
    EXPECT_CALL(*connectionPool, createConnection()).Times(connectionNumber);
    initConnections();
}

TEST_F(ConnectionPoolTest, shouldCloseAllConnectionsOnDestruction)
{
    initConnections();
    ASSERT_EQ(connectionNumber, ConnectionMock::openConnections);
    connectionPool.reset();
    ASSERT_EQ(0u, ConnectionMock::openConnections);
}

TEST_F(ConnectionPoolTest, shouldThrowExceptionWhenCreatedWith0Connections)
{
    auto createConnectionPool = [&]{ ConnectionPoolProxy{0u, uri}; };
    EXPECT_THROW(createConnectionPool(), std::invalid_argument);
}

TEST_F(ConnectionPoolTest, shouldCallOnMessageCallbackOnReceivedMessage)
{
    std::string message;
    std::generate_n(std::back_inserter(message), intDis(gen), [&]{return charDis(gen);});

    initConnections();
    EXPECT_CALL(*this, onMessage(message)).Times(1);
    connectionPool->createdConnections.front()->m_onMessageCallback(message);
}

TEST_F(ConnectionPoolTest, shouldDoNothingOnReceiveWhenReceiveMessageIsUnset)
{
    std::string message;
    std::generate_n(std::back_inserter(message), intDis(gen), [&]{return charDis(gen);});

    connectionPool = std::make_unique<ConnectionPoolProxy>(connectionNumber, uri);
    ON_CALL(*connectionPool, createConnection()).WillByDefault(
                Invoke(connectionPool.get(), &ConnectionPoolProxy::createConnection_impl));

    initConnections();
    EXPECT_CALL(*this, onMessage(_)).Times(0);
    connectionPool->createdConnections.front()->m_onMessageCallback(message);

    EXPECT_NO_THROW(connectionPool->m_onMessageCallback(message));
}

TEST_F(ConnectionPoolTest, shouldDropConnectionsOnError)
{
    std::uniform_int_distribution<unsigned int> droppedDis{1, connectionNumber};
    auto dropConnections = droppedDis(gen);
    initConnections();

    for(auto i = 0u; i < dropConnections; ++i)
    {
        auto c = connectionPool->createdConnections.back();
        connectionPool->createdConnections.pop_back();
        c->m_onErrorCallback();
    }

    ASSERT_EQ(connectionNumber - dropConnections, ConnectionMock::openConnections);
}

TEST_F(ConnectionPoolTest, shouldDropConnectionsOnOpenFailure)
{
    std::uniform_int_distribution<unsigned int> droppedDis{1, connectionNumber};
    auto dropConnections = droppedDis(gen);
    connectionPool->addConnections();

    for(auto i = 0u; i < dropConnections; ++i)
    {
        auto c = connectionPool->createdConnections.back();
        connectionPool->createdConnections.pop_back();
        c->m_onFailCallback();
    }
    for(auto &c: connectionPool->createdConnections)
        c->m_onOpenCallback();

    ASSERT_EQ(connectionNumber - dropConnections, ConnectionMock::openConnections);
}

TEST_F(ConnectionPoolTest, shouldRecreateConnectionsOnSend)
{
    std::uniform_int_distribution<unsigned int> droppedDis{1, connectionNumber};
    auto dropConnections = droppedDis(gen);
    initConnections();

    connectionPool->send("");

    ASSERT_EQ(connectionNumber, ConnectionMock::openConnections);
}

//TEST_F(ConnectionPoolTest, shouldWaitOnSendUntilConnectionIsAvailable)
//{
//    connectionPool->send("");
//}

ACTION_P(Increment, counter) { ++*counter; }

TEST_F(ConnectionPoolTest, shouldPassMessageToOneOfItsConnections)
{
    std::string message;
    std::generate_n(std::back_inserter(message), intDis(gen), [&]{return charDis(gen);});

    std::atomic<unsigned int> sentMessages{0};
    initConnections();

    for(auto &c: connectionPool->createdConnections)
        EXPECT_CALL(*c, send(message)).WillRepeatedly(Increment(&sentMessages));

    for(auto i = 1u; i <= intDis(gen); ++i)
    {
        connectionPool->send(message);
        ASSERT_EQ(i, sentMessages);
    }
}

TEST_F(ConnectionPoolTest, shouldDistributeMessagesEvenlyAcrossConnections)
{
    std::string message;
    std::generate_n(std::back_inserter(message), intDis(gen), [&]{return charDis(gen);});

    initConnections();

    for(auto &c: connectionPool->createdConnections)
        EXPECT_CALL(*c, send(message)).Times(3);

    for(auto i = 0u; i < connectionNumber * 3; ++i)
        connectionPool->send(message);
}
