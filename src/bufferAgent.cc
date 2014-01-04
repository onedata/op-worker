#include "bufferAgent.h"
#include "helpers/storageHelperFactory.h"
#include "glog/logging.h"

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
    {
        unique_lock guard(m_wrMutex);
        m_wrCacheMap.erase(ffi->fh);

        write_buffer_ptr lCache(new WriteCache());
        lCache->fileName = path;
        lCache->buffer = newFileCache(true);
        lCache->ffi = *ffi;

        m_wrCacheMap[ffi->fh] = lCache;
    }

    {
        unique_lock guard(m_rdMutex);
        read_cache_map_t::iterator it;
        if(( it = m_rdCacheMap.find(path) ) != m_rdCacheMap.end()) {
            it->second->openCount++;
        } else {
            read_buffer_ptr lCache(new ReadCache());
            lCache->fileName = path;
            lCache->openCount = 1;
            lCache->ffi = *ffi;
            lCache->buffer = newFileCache(false);

            m_rdCacheMap[path] = lCache;
        }

        m_rdJobQueue.push_front(PrefetchJob(path, 0, 512));
        m_rdCond.notify_one();
    }

    return 0;
}

int BufferAgent::onWrite(std::string path, const std::string &buf, size_t size, off_t offset, ffi_type ffi)
{
    unique_lock guard(m_wrMutex);
        write_buffer_ptr wrapper = m_wrCacheMap[ffi->fh];
    guard.unlock();

    {
        if(wrapper->buffer->byteSize() > 1024 * 1024 * 64) {
            if(int fRet = onFlush(path, ffi)) {
                return fRet;
            }
        }

        if(wrapper->buffer->byteSize() > 1024 * 1024 * 64) {
            return doWrite(path, buf, size, offset, ffi);
        }

        wrapper->buffer->writeData(offset, buf);
    }
    
    unique_lock buffGuard(wrapper->mutex);
    guard.lock();
    if(!wrapper->opPending) 
    {
        wrapper->opPending = true;
        m_wrJobQueue.push_back(ffi->fh);
        m_wrCond.notify_one();
    }

    return size;
}

int BufferAgent::onRead(std::string path, std::string &buf, size_t size, off_t offset, ffi_type ffi)
{
    unique_lock guard(m_rdMutex);
        read_buffer_ptr wrapper = m_rdCacheMap[path];
    guard.unlock();

    wrapper->buffer->readData(offset, size, buf);

    if(buf.size() < size) {
        string buf2;
        int ret = doRead(path, buf2, size - buf.size(), offset + buf.size(), &wrapper->ffi);
        if(ret < 0)
            return ret;

        buf += buf2;

        DLOG(INFO) << "doRead ret: " << ret << " bufSize: " << buf2.size() << " globalBufSize: " << buf.size() ;

        {   
            unique_lock buffGuard(wrapper->mutex);
            wrapper->blockSize = std::min((size_t) 1024 * 1024, (size_t) std::max(size, 2*wrapper->blockSize));
        }

        guard.lock();
            m_rdJobQueue.push_back(PrefetchJob(wrapper->fileName, buf.size(), wrapper->blockSize));
        guard.unlock();
    } else {
        string tmp;
        size_t prefSize = std::max(2*size, wrapper->blockSize);
        wrapper->buffer->readData(offset + size, prefSize, tmp);

        if(tmp.size() != prefSize) {
            guard.lock();
                m_rdJobQueue.push_back(PrefetchJob(wrapper->fileName, offset + size + tmp.size(), wrapper->blockSize));
                m_rdJobQueue.push_back(PrefetchJob(wrapper->fileName, offset + size + tmp.size() + wrapper->blockSize, wrapper->blockSize));
            guard.unlock();
        }
    }

    m_rdCond.notify_one();

    return buf.size();
}

int BufferAgent::onFlush(std::string path, ffi_type ffi)
{
    unique_lock guard(m_wrMutex);
        write_buffer_ptr wrapper = m_wrCacheMap[ffi->fh];
    guard.unlock();

    unique_lock sendGuard(wrapper->sendMutex);
    unique_lock buff_guard(wrapper->mutex);

    while(wrapper->buffer->blockCount() > 0) 
    {
        block_ptr block = wrapper->buffer->removeOldestBlock();
        uint64_t start = utils::mtime<uint64_t>();
        int res = doWrite(wrapper->fileName, block->data, block->data.size(), block->offset, &wrapper->ffi);
        uint64_t end = utils::mtime<uint64_t>();

        LOG(INFO) << "Roundtrip: " << (end - start) << " for " << block->data.size() << " bytes";
        
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
    m_wrJobQueue.remove(ffi->fh);

    return 0;
}

int BufferAgent::onRelease(std::string path, ffi_type ffi)
{
    {
        unique_lock guard(m_wrMutex);
        m_wrCacheMap.erase(ffi->fh);
        m_wrJobQueue.remove(ffi->fh);
    }

    {
        unique_lock guard(m_rdMutex);

        read_cache_map_t::iterator it;
        if(( it = m_rdCacheMap.find(path) ) != m_rdCacheMap.end()) {
            it->second->openCount--;
            if(it->second->openCount <= 0) {
                m_rdCacheMap.erase(it);
            }
        }
    }

    return 0;
}



void BufferAgent::agentStart(int worker_count)
{
    m_workers.clear();
    m_agentActive = true;

    while(worker_count--)
    {
        m_workers.push_back(shared_ptr<thread>(new thread(bind(&BufferAgent::writerLoop, this))));
        m_workers.push_back(shared_ptr<thread>(new thread(bind(&BufferAgent::readerLoop, this))));
    }
}

void BufferAgent::agentStop()
{
    m_agentActive = false;
    m_wrCond.notify_all();
    m_rdCond.notify_all();

    while(m_workers.size() > 0)
    {
        m_workers.back()->join();
        m_workers.pop_back();
    }
}

void BufferAgent::readerLoop() 
{
    unique_lock guard(m_rdMutex);
    while(m_agentActive)
    {
        while(m_rdJobQueue.empty() && m_agentActive)
            m_rdCond.wait(guard);

        if(!m_agentActive)
            return;

        PrefetchJob job = m_rdJobQueue.front();
        read_buffer_ptr wrapper = m_rdCacheMap[job.fileName];
        m_rdJobQueue.pop_front();
        m_rdCond.notify_one();

        if(!wrapper)
            continue;

        guard.unlock();

        {
            string buff;
            int ret = doRead(wrapper->fileName, buff, job.size, job.offset, &wrapper->ffi);
            if(ret > 0 && buff.size() >= ret) {
                wrapper->buffer->writeData(job.offset, buff);
            }
        }

        guard.lock();
    }
}

void BufferAgent::writerLoop()
{
    unique_lock guard(m_wrMutex);
    while(m_agentActive)
    {
        while(m_wrJobQueue.empty() && m_agentActive)
            m_wrCond.wait(guard);

        if(!m_agentActive)
            return;

        fd_type file = m_wrJobQueue.front();
        write_buffer_ptr wrapper = m_wrCacheMap[file];
        m_wrJobQueue.pop_front();
        m_wrCond.notify_one();

        if(!wrapper)
            continue;

        guard.unlock();

        {
            unique_lock sendGuard(wrapper->sendMutex);

            block_ptr block;
            {
                unique_lock buff_guard(wrapper->mutex);
                block = wrapper->buffer->removeOldestBlock();
            } 

            if(block) 
            {
                uint64_t start = utils::mtime<uint64_t>();
                int res = doWrite(wrapper->fileName, block->data, block->data.size(), block->offset, &wrapper->ffi);
                uint64_t end = utils::mtime<uint64_t>();

                LOG(INFO) << "Roundtrip: " << (end - start) << " for " << block->data.size() << " bytes";
 
                wrapper->cond.notify_all();
            }

            {
                unique_lock buff_guard(wrapper->mutex);
                guard.lock();
                if(wrapper->buffer->blockCount() > 0)
                {
                    m_wrJobQueue.push_back(file);
                } 
                else 
                {
                    wrapper->opPending = false;
                }
            } 
        }
        
        wrapper->cond.notify_all();
    }
}

boost::shared_ptr<FileCache> BufferAgent::newFileCache(bool isBuffer)
{
    return boost::shared_ptr<FileCache>(new FileCache(10 * 1024 * 1024, isBuffer));
}

} // namespace helpers 
} // namespace veil

