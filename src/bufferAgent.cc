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

int BufferAgent::onOpen(std::string path, ffi_type ffi)
{
    unique_lock guard(m_loopMutex);
    m_cacheMap.erase(ffi->fh);

    buffer_ptr lCache(new LockableCache());
    lCache->fileName = path;
    lCache->buffer = newFileCache();
    lCache->ffi = *ffi;

    m_cacheMap[ffi->fh] = lCache;

    return 0;
}

int BufferAgent::onWrite(std::string path, const std::string &buf, size_t size, off_t offset, ffi_type ffi)
{
    unique_lock guard(m_loopMutex);
        buffer_ptr wrapper = m_cacheMap[ffi->fh];
    guard.unlock();
    
    unique_lock buff_guard(wrapper->mutex);

    while(wrapper->buffer->byteSize() > 1024 * 1024 * 10) {
        guard.lock();
            m_jobQueue.push_front(ffi->fh);
            m_loopCond.notify_all();
        guard.unlock();
        wrapper->cond.wait(buff_guard);
    }

    wrapper->buffer->writeData(offset, buf);

    guard.lock();
    m_jobQueue.push_back(ffi->fh);
    m_loopCond.notify_all();

    return size;
}

int BufferAgent::onRead(std::string path, std::string &buf, size_t size, off_t offset, ffi_type ffi)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_loopMutex);
    return EOPNOTSUPP;
}

int BufferAgent::onFlush(std::string path, ffi_type ffi)
{
    unique_lock guard(m_loopMutex);
        buffer_ptr wrapper = m_cacheMap[ffi->fh];
    guard.unlock();

    unique_lock buff_guard(wrapper->mutex);

    while(wrapper->buffer->blockCount() > 0) 
    {
        block_ptr block = wrapper->buffer->removeOldestBlock();
        int res = doWrite(wrapper->fileName, block->data, block->data.size(), block->offset, &wrapper->ffi);
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
    m_jobQueue.remove(ffi->fh);

    return 0;
}

int BufferAgent::onRelease(std::string path, ffi_type ffi)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_loopMutex);
    m_cacheMap.erase(ffi->fh);

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
    m_loopCond.notify_all();
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
        while(m_jobQueue.empty() && m_agentActive)
            m_loopCond.wait(guard);

        if(!m_agentActive)
            return;

        fd_type file = m_jobQueue.front();
        buffer_ptr wrapper = m_cacheMap[file];
        m_jobQueue.pop_front();
        m_loopCond.notify_all();

        if(!wrapper)
            continue;

        
        {
            guard.unlock();
            block_ptr block;
            {
                unique_lock buff_guard(wrapper->mutex);
                block = wrapper->buffer->removeOldestBlock();
            } 

            if(block) 
            {
                //int res = doWrite(wrapper->fileName, block->data, block->data.size(), block->offset, &wrapper->ffi);
            
                wrapper->cond.notify_all();
            }

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
    return boost::shared_ptr<FileCache>(new FileCache(500 * 1024));
}

} // namespace helpers 
} // namespace veil

