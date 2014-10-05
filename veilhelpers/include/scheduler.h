/**
 * @file scheduler.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef HELPERS_SCHEDULER_H
#define HELPERS_SCHEDULER_H


#include <chrono>
#include <functional>
#include <vector>
#include <thread>

#include <boost/asio/io_service.hpp>

namespace one
{

/**
 * The Scheduler class is responsible for scheduling work to an underlying set
 * of worker threads.
 */
class Scheduler
{
public:
    /**
     * Constructor.
     * Creates worker threads.
     * @param threadNumber The number of threads to be spawned.
     */
    Scheduler(const unsigned int threadNumber);

    /**
     * Destructor.
     * Stops the scheduler and joins worker threads.
     */
    ~Scheduler();

    /**
     * Schedules a task tu be run after some time.
     * @param after The duration after which the task should be executed.
     * @param task The task to execute.
     */
    void schedule(const std::chrono::milliseconds after,
                  std::function<void()> task);

private:
    std::vector<std::thread> m_workers;
    boost::asio::io_service m_ioService;
    boost::asio::io_service::work m_idleWork;
};

} // namespace one

#endif // HELPERS_SCHEDULER_H
