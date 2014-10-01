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
 * The Scheduler class is responsible for scheduling work to an underlying pool
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
    virtual ~Scheduler();

    /**
     * Runs a task asynchronously in @c Scheduler's thread pool.
     * @param task The task to execute.
     */
    virtual void post(const std::function<void()> &task);

    /**
     * Runs a task asynchronously in @c Scheduler's thread pool on an object
     * references by a non-owning pointer.
     * @param member The member to invoke.
     * @param subject The subject whose member is to be invoked.
     * @param args Arguments to pass to the member.
     */
    template<class R, class T, class... Args>
    void post(R (T::*member), const std::weak_ptr<T> &subject, Args&&... args)
    {
        post([=]{
            if(auto s = subject.lock())
                ((*s).*member)(args...);
        });
    }

    /**
     * A convenience overload for @c post taking a @c std::shared_ptr.
     */
    template<class R, class T, class... Args>
    void post(R (T::*member), const std::shared_ptr<T> &subject, Args&&... args)
    {
        post(member, std::weak_ptr<T>{subject}, std::forward<Args>(args)...);
    }

    /**
     * Schedules a task to be run after some time.
     * @param after The duration after which the task should be executed.
     * @param task The task to execute.
     * @return A function to cancel the scheduled task.
     */
    virtual std::function<void()> schedule(const std::chrono::milliseconds after,
                                           std::function<void()> task);

    /**
     * Schedules a task to be run after some time on an object referenced by a
     * non-owning pointer.
     * @param after The duration after which the task should be executed.
     * @param member The member to invoke.
     * @param subject The subject whose member is to be invoked.
     * @param args Arguments to pass to the member.
     * @return A function to cancel the scheduled task.
     */
    template<class R, class T, class... Args>
    std::function<void()> schedule(const std::chrono::milliseconds after,
                                   R (T::*member),
                                   std::weak_ptr<T> subject,
                                   Args&&... args)
    {
        return schedule(after, [=]{
            if(auto s = subject.lock())
                ((*s).*member)(args...);
        });
    }

    /**
     * A convenience overload for @c schedule taking a @c std::shared_ptr.
     */
    template<class R, class T, class... Args>
    std::function<void()> schedule(const std::chrono::milliseconds after,
                                   R (T::*member),
                                   std::shared_ptr<T> subject,
                                   Args&&... args)
    {
        return schedule(after, member,
                        std::weak_ptr<T>{subject}, std::forward<Args>(args)...);
    }

private:
    std::vector<std::thread> m_workers;
    boost::asio::io_service m_ioService;
    boost::asio::io_service::work m_idleWork;
};

} // namespace veil


#endif // VEILHELPERS_SCHEDULER_H
