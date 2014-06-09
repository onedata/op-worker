/**
 * @file remoteLogWriter_mock.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef REMOTE_LOG_WRITER_MOCK_H
#define REMOTE_LOG_WRITER_MOCK_H


#include "logging.h"
#include <gmock/gmock.h>

using namespace veil::logging;

class MockRemoteLogWriter: public RemoteLogWriter
{
public:
    MockRemoteLogWriter() : RemoteLogWriter{nullptr} {}

    MOCK_METHOD5(buffer, void(const RemoteLogLevel, const std::string&,
                              const int, const time_t, const std::string&));

    MOCK_METHOD1(handleThresholdChange, bool(const protocol::communication_protocol::Answer&));
};


#endif // REMOTE_LOG_WRITER_MOCK_H
