/*
 * Copyright 2000-2014 NeuStar, Inc. All rights reserved.
 * NeuStar, the Neustar logo and related names and logos are registered
 * trademarks, service marks or tradenames of NeuStar, Inc. All other
 * product names, company names, marks, logos and symbols may be trademarks
 * of their respective owners.
 */

/*
 * simple_scheduled_thread_pool_executor.h
 *
 *  Created on: Jun 11, 2014
 *      Author: vpoliboy
 */

#ifndef SIMPLE_SCHEDULED_THREAD_POOL_EXECUTOR_H_
#define SIMPLE_SCHEDULED_THREAD_POOL_EXECUTOR_H_

#include <asio.hpp>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace cppmetrics {
namespace concurrent {

/**
 * A simple threadpool that executes a given command at a given interval of
 * time.
 */
class SimpleScheduledThreadPoolExecutor {
public:
    /**
     * Creates a new instance with the given thread size.
     * @param pool_size The number of threads in the threadpool.
     */
    SimpleScheduledThreadPoolExecutor(size_t pool_size);

    virtual ~SimpleScheduledThreadPoolExecutor();

    /**
     * Executes the give task at the configured interval rate until shutdown is
     * called. The given command is executed at a fixed rate and therefore there
     * might be more than one command running at a time depending on the
     * duration of the command.
     * @param command The command to execute at fixed interval.
     * @param period The interval between the start of the tasks.
     */
    virtual void scheduleAtFixedRate(
        std::function<void()> command, std::chrono::milliseconds period);

    /**
     * Executes the give task at the configured interval delay until shutdown is
     * called. The given command is executed at a fixed delay. There can be only
     * one task instance running at a given time.
     * @param command The command to execute at fixed delay.
     * @param period The time period between the end of the tasks.
     */
    virtual void scheduleAtFixedDelay(
        std::function<void()> command, std::chrono::milliseconds period);

    /**
     * Shuts down the service, may or may not return immediately depending on
     * the pending tasks.
     */
    virtual void shutdown();

    /**
     * Shuts down the service, will return immediately.
     */
    virtual void shutdownNow();

    /**
     * gets the threadpool state.
     * @return True if this is shutdown or shutting down, false otherwise.
     */
    virtual bool isShutdown() const;

private:
    void cancelTimers();
    void timerHandler(const asio::error_code &ec, size_t timer_index);

    void scheduleTimer(std::function<void()> task,
        std::chrono::milliseconds period, bool fixed_rate);

    std::atomic<bool> running_;
    asio::io_service io_service_;
    // This asio work pointer will keep the io_service alive
    std::unique_ptr<asio::io_service::work> work_ptr_;
    std::vector<std::thread> thread_group_;

    class TimerTask;
    typedef std::vector<TimerTask> TimerTasks;
    TimerTasks timer_tasks_;
    mutable std::mutex timer_task_mutex_;
};

} /* namespace concurrent */
} /* namespace cppmetrics */
#endif /* SIMPLE_SCHEDULED_THREAD_POOL_EXECUTOR_H_ */
