/**
 * @file scheduler.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "scheduler.h"

namespace veil
{

Scheduler::Scheduler(const unsigned int threadNumber)
    : m_idleWork{m_ioService}
{
    for(auto i = 0u; i < threadNumber; ++i)
        m_workers.emplace_back([this]{ m_ioService.run(); });
}

Scheduler::~Scheduler()
{
    m_ioService.stop();
    for(auto &t: m_workers)
        t.join();
}

void Scheduler::post(std::function<void()> task)
{
    m_ioService.post(task);
}

void Scheduler::handle(const boost::system::error_code &error,
                       std::function<void()> callback,
                       std::shared_ptr<steady_timer> /*timer*/)
{
    if(!error)
        callback();
}

} // namespace veil
