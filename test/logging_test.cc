/**
 * @file logging_test.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "connectionPool_mock.h"
#include "communicationHandler_mock.h"
#include "logging.h"
#include "logging.pb.h"
#include "remoteLogWriter_mock.h"
#include "testCommonH.h"
#include "helpers/storageHelperFactory.h"

#include <boost/atomic.hpp>
#include <boost/shared_ptr.hpp>

#include <unistd.h>

#include <ctime>

struct RemoteLogSinkFixture: public ::testing::Test
{
    RemoteLogSinkFixture()
        : mockLogWriter(new NiceMock<MockRemoteLogWriter>)
        , logSink(mockLogWriter)
    {
    }

    boost::shared_ptr<NiceMock<MockRemoteLogWriter> > mockLogWriter;
    veil::logging::RemoteLogSink logSink;
};

struct RemoteLogWriterFixture: public ::testing::Test
{
    RemoteLogWriterFixture()
        : mockConnectionPool(new NiceMock<MockConnectionPool>)
        , mockCommunicationHandler(new NiceMock<MockCommunicationHandler>)
        , logWriter(veil::protocol::logging::LDEBUG)
    {
        veil::helpers::config::setConnectionPool(mockConnectionPool);
        ON_CALL(*mockConnectionPool, selectConnection(_))
                .WillByDefault(::testing::Return(mockCommunicationHandler));

        logWriter.run();
    }

    bool areMessagesEqual(const veil::protocol::logging::LogMessage &l,
                          const veil::protocol::logging::LogMessage &r)
    {
        return l.level() == r.level() && l.file_name() == r.file_name() &&
               l.line() == r.line() && l.timestamp() == r.timestamp() &&
               l.message() == r.message();
    }

    template<typename pred>
    bool waitUntil(const pred &condition, const time_t timeout)
    {
        const time_t until = std::time(0) + timeout;
        while(!condition && std::time(0) < until); // busy wait
        return condition;
    }

    veil::protocol::logging::LogMessage
    sendMessage(const RemoteLogLevel level, const std::string &fileName,
                const int line, const time_t timestamp, const std::string &message)
    {
        using namespace ::testing;

        boost::atomic<bool> messageSent(false);
        veil::protocol::communication_protocol::ClusterMsg sentClsMessage;

        EXPECT_CALL(*mockCommunicationHandler, sendMessage(_, _))
                .WillOnce(DoAll(SaveArg<0>(&sentClsMessage), Assign(&messageSent, true), Return(0)));

        logWriter.buffer(level, fileName, line, timestamp, message);
        EXPECT_TRUE(waitUntil(messageSent, 10));

        veil::protocol::logging::LogMessage sentMessage;
        EXPECT_TRUE(sentMessage.ParseFromString(sentClsMessage.input()));

        return sentMessage;
    }

    boost::shared_ptr<NiceMock<MockConnectionPool> > mockConnectionPool;
    boost::shared_ptr<NiceMock<MockCommunicationHandler> > mockCommunicationHandler;
    RemoteLogWriter logWriter;
};

INIT_AND_RUN_ALL_TESTS()

TEST_F(RemoteLogSinkFixture, ShouldBeUsableByGlog)
{
    LOG_TO_SINK_BUT_NOT_TO_LOGFILE(&logSink, INFO) << "Hello";
}

TEST_F(RemoteLogSinkFixture, ShouldPassLogMessagesToLogWriter)
{
    const std::string message = "Hello!";
    EXPECT_CALL(*mockLogWriter, buffer(_, _, _, _, message));

    LOG_TO_SINK_BUT_NOT_TO_LOGFILE(&logSink, INFO) << message;
}

TEST_F(RemoteLogSinkFixture, ShouldDirectlyTranslateSeverityByDefault)
{
    using namespace veil;

    const std::string infoMessage = "Hello World!";
    const std::string warningMessage = "I warn you, World!";
    const std::string errorMessage = "You've made an error, World!";
    const std::string fatalMessage = "kill all humans";

    EXPECT_CALL(*mockLogWriter, buffer(protocol::logging::INFO, _, _, _, infoMessage));
    EXPECT_CALL(*mockLogWriter, buffer(protocol::logging::WARNING, _, _, _, warningMessage));
    EXPECT_CALL(*mockLogWriter, buffer(protocol::logging::ERROR, _, _, _, errorMessage));
    EXPECT_CALL(*mockLogWriter, buffer(protocol::logging::FATAL, _, _, _, fatalMessage));

    LOG_TO_SINK_BUT_NOT_TO_LOGFILE(&logSink, INFO) << infoMessage;
    LOG_TO_SINK_BUT_NOT_TO_LOGFILE(&logSink, WARNING) << warningMessage;
    LOG_TO_SINK_BUT_NOT_TO_LOGFILE(&logSink, ERROR) << errorMessage;
    LOG_TO_SINK_BUT_NOT_TO_LOGFILE(&logSink, FATAL) << fatalMessage;
}

TEST_F(RemoteLogSinkFixture, ShouldOverrideSeverityIfSetInConstructor)
{
    using namespace veil;

    const std::string infoMessage = "Hello World Debug!";
    const std::string warningMessage = "I debug you, World!";

    EXPECT_CALL(*mockLogWriter, buffer(protocol::logging::LDEBUG, _, _, _, infoMessage));
    EXPECT_CALL(*mockLogWriter, buffer(protocol::logging::LDEBUG, _, _, _, warningMessage));

    logging::RemoteLogSink debugLogSink(mockLogWriter, protocol::logging::LDEBUG);

    LOG_TO_SINK_BUT_NOT_TO_LOGFILE(&debugLogSink, INFO) << infoMessage;
    LOG_TO_SINK_BUT_NOT_TO_LOGFILE(&debugLogSink, WARNING) << warningMessage;
}

TEST(RemoteLogWriter, ShouldNotHangOnDestroy)
{
    veil::logging::RemoteLogWriter logWriter;
    logWriter.run();
}

TEST_F(RemoteLogWriterFixture, ShouldSendReceivedMessagesThroughConnection)
{
    veil::protocol::logging::LogMessage expectedMessage;
    expectedMessage.set_level(veil::protocol::logging::INFO);
    expectedMessage.set_file_name("meaningful_file.cc");
    expectedMessage.set_line(42);
    expectedMessage.set_timestamp(std::time(0));
    expectedMessage.set_message("a meaningful INFO message");

    const veil::protocol::logging::LogMessage sentMessage =
            sendMessage(expectedMessage.level(), expectedMessage.file_name(),
                        expectedMessage.line(), expectedMessage.timestamp(),
                        expectedMessage.message());

    EXPECT_TRUE(areMessagesEqual(expectedMessage, sentMessage));
}

TEST_F(RemoteLogWriterFixture, ShouldSendCurrentPIDInTheMessage)
{
    const veil::protocol::logging::LogMessage sentMessage =
            sendMessage(veil::protocol::logging::INFO, "", 0, 0, "");

    EXPECT_EQ(getpid(), static_cast<pid_t>(sentMessage.pid()));
}

TEST_F(RemoteLogWriterFixture, ShouldSendAMessageWithIGNORE_ANSWER_MSG_ID)
{
    using namespace ::testing;

    boost::atomic<bool> messageSent(false);

    EXPECT_CALL(*mockCommunicationHandler, sendMessage(_, IGNORE_ANSWER_MSG_ID))
            .WillOnce(DoAll(Assign(&messageSent, true), Return(0)));

    logWriter.buffer(veil::protocol::logging::INFO, "", 0, 0, "");

    waitUntil(messageSent, 10);
}

TEST_F(RemoteLogWriterFixture, ShouldDropMessagesAfterExceedingMaxBufferSize)
{
    veil::logging::RemoteLogWriter writer(veil::protocol::logging::LDEBUG,
                                          /*maxBufferSize*/ 10,
                                          /*bufferTrimSize*/ 5);

    // The writer.run() is not called yet so the write loop is not running.
    for(int i = 0; i < 11; ++i)
        writer.buffer(veil::protocol::logging::INFO, "", 0, 0, "");

    // We expect the writer to send 6 messages total, the last one being a
    // warning about dropped messages.
    boost::atomic<bool> messageSent(false);
    veil::protocol::communication_protocol::ClusterMsg sentClsMessage;

    // Note: gmock satisfies expectations in reverse order
    EXPECT_CALL(*mockCommunicationHandler, sendMessage(_, _))
            .WillOnce(DoAll(SaveArg<0>(&sentClsMessage), Assign(&messageSent, true), Return(0)));
    EXPECT_CALL(*mockCommunicationHandler, sendMessage(_, _))
            .Times(5).WillRepeatedly(Return(0)).RetiresOnSaturation();

    writer.run();
    waitUntil(messageSent, 10);

    veil::protocol::logging::LogMessage log;
    log.ParseFromString(sentClsMessage.input());

    EXPECT_EQ(veil::protocol::logging::WARNING, log.level());
    EXPECT_NE(std::string::npos, log.message().find("buffered messages has been exceeded"));
}
