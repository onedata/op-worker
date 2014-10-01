/**
 * @file scheduler.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_SCHEDULER_H
#define VEILHELPERS_SCHEDULER_H


#include <boost/asio.hpp>

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
    /// Force boost's steady_timer to use std::chrono.
    using steady_timer = boost::asio::basic_waitable_timer<std::chrono::steady_clock>;

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
     * Runs a task asynchronously in @c Scheduler's thread pool.
     * @param task The task to execute.
     */
    void post(std::function<void()> task);

    /**
     * Schedules a task to be run after some time.
     * @param after The duration after which the task should be executed.
     * @param task The task to execute.
     * @return A function to cancel the scheduled task.
     */
    template<class Rep, class Period>
    std::function<void()> schedule(const std::chrono::duration<Rep, Period> after,
                                   std::function<void()> task)
    {
        using namespace std::placeholders;
        const auto timer = std::make_shared<steady_timer>(m_ioService, after);
        timer->async_wait(std::bind(&Scheduler::handle, this, _1,
                                    std::move(task), timer));

        std::weak_ptr<steady_timer> weakTimer;
        return [weakTimer]{
            if(auto t = weakTimer.lock())
                t->cancel();
        };
    }

private:
    // The timer argument serves to preserve timer's life until the handle
    // function is called.
    void handle(const boost::system::error_code &error,
                std::function<void()> callback,
                std::shared_ptr<steady_timer> /*timer*/);

    std::vector<std::thread> m_workers;
    boost::asio::io_service m_ioService;
    boost::asio::io_service::work m_idleWork;
};

} // namespace veil


#endif // VEILHELPERS_SCHEDULER_H
