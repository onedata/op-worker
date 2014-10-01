/**
 * @file testUtils.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_SCHEDULER_MOCK_H
#define VEILHELPERS_SCHEDULER_MOCK_H


#include "scheduler.h"

#include <gmock/gmock.h>

class MockScheduler: public veil::Scheduler
{
public:
    MockScheduler()
        : veil::Scheduler{0}
    {
    }

    MOCK_METHOD1(post, void(const std::function<void()>&));
    MOCK_METHOD2(schedule, std::function<void()>(const std::chrono::milliseconds,
                                                 std::function<void()>));
};


#endif // VEILHELPERS_SCHEDULER_MOCK_H
