/**
 * @file scheduler.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_SCHEDULER_H
#define VEILHELPERS_SCHEDULER_H


#include <boost/asio/io_service.hpp>

#include <chrono>
#include <functional>
#include <vector>
#include <thread>

namespace veil
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
     * Schedules a task to be run after some time.
     * @param after The duration after which the task should be executed.
     * @param task The task to execute.
     * @return A function to cancel the scheduled task.
     */
    std::function<void()> schedule(const std::chrono::milliseconds after,
                                   std::function<void()> task);

private:
    std::vector<std::thread> m_workers;
    boost::asio::io_service m_ioService;
    boost::asio::io_service::work m_idleWork;
};

} // namespace veil

#endif // VEILHELPERS_SCHEDULER_H
