#include "bufferAgent.h"

using boost::shared_ptr;
using boost::thread;
using boost::bind;

using std::string;

namespace veil {
namespace helpers {

BufferAgent::BufferAgent(write_fun w, read_fun r)
  : m_agentActive(false),
    doWrite(w),
    doRead(r)
{
    agentStart();
}
BufferAgent::~BufferAgent()
{
    agentStop();
}

int BufferAgent::onOpen(std::string path, fd_type fd)
{
    unique_lock guard(m_loopMutex);
    m_cacheMap.erase(fd);

    buffer_ptr lCache(new LockableCache());
    lCache->fileName = path;
    lCache->buffer = newFileCache();

    m_cacheMap[fd] = lCache;

    return 0;
}

int BufferAgent::onWrite(std::string path, const std::string &buf, size_t size, off_t offset, fd_type fd)
{
    unique_lock guard(m_loopMutex);
        buffer_ptr wrapper = m_cacheMap[fd];
    guard.release();

    unique_lock buff_guard(wrapper->mutex);

    while(wrapper->buffer->byteSize() > 1024 * 1024 * 10) {
        wrapper->cond.wait(buff_guard);
    }

    wrapper->buffer->writeData(offset, buf);

    guard.lock();
    m_jobQueue.push_back(fd);

    return EOPNOTSUPP;
}

int BufferAgent::onRead(std::string path, std::string &buf, size_t size, off_t offset, fd_type file)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_loopMutex);
    return EOPNOTSUPP;
}

int BufferAgent::onFlush(std::string path, fd_type file)
{
    unique_lock guard(m_loopMutex);
        buffer_ptr wrapper = m_cacheMap[file];
    guard.release();

    unique_lock buff_guard(wrapper->mutex);

    while(wrapper->buffer->blockCount() > 0) 
    {
        block_ptr block = wrapper->buffer->removeOldestBlock();
        int res = doWrite(wrapper->fileName, block->data, block->data.size(), block->offset, file);
        if(res < 0)
        {
            while(wrapper->buffer->blockCount() > 0)
            {
                (void) wrapper->buffer->removeOldestBlock();
            }
            return res;
        }
    }

    guard.lock();
    m_jobQueue.remove(file);

    return 0;
}

int BufferAgent::onRelease(std::string path, fd_type fd)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_loopMutex);
    m_cacheMap.erase(fd);

    return 0;
}



void BufferAgent::agentStart(int worker_count)
{
    m_workers.clear();
    m_agentActive = true;

    while(worker_count--)
    {
        m_workers.push_back(shared_ptr<thread>(new thread(bind(&BufferAgent::workerLoop, this))));
    }
}

void BufferAgent::agentStop()
{
    m_agentActive = false;
    while(m_workers.size() > 0)
    {
        m_workers.back()->join();
        m_workers.pop_back();
    }
}

void BufferAgent::workerLoop()
{
    unique_lock guard(m_loopMutex);
    while(m_agentActive)
    {
        while(m_jobQueue.empty())
            m_loopCond.wait(guard);

        fd_type file = m_jobQueue.front();
        buffer_ptr wrapper = m_cacheMap[file];
        m_jobQueue.pop_front();
        m_loopCond.notify_all();

        if(!wrapper)
            continue;

        
        {
            guard.release();

            unique_lock buff_guard(wrapper->mutex);

            block_ptr block = wrapper->buffer->removeOldestBlock();
            int res = doWrite(wrapper->fileName, block->data, block->data.size(), block->offset, file);
            
            wrapper->cond.notify_all();

            guard.lock();
            if(wrapper->buffer->blockCount() > 0)
            {
                m_jobQueue.push_back(file);
            }
        }
        

    }
}

boost::shared_ptr<FileCache> BufferAgent::newFileCache()
{
    return boost::shared_ptr<FileCache>(new FileCache(100 * 1024));
}

} // namespace helpers 
} // namespace veil

