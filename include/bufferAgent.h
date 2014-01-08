#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <string>
#include <fuse.h>
#include <unistd.h>
#include <fcntl.h>

#include "fileCache.h"


namespace veil {
namespace helpers {

typedef struct fuse_file_info*  ffi_type;
typedef uint64_t                fd_type;

typedef boost::function<int(std::string path, const std::string &buf, size_t, off_t, ffi_type)>    write_fun;
typedef boost::function<int(std::string path, std::string &buf, size_t, off_t, ffi_type)>          read_fun;

typedef boost::unique_lock<boost::recursive_mutex> unique_lock;

class BufferAgent
{
public:

    struct WriteCache {
        boost::shared_ptr<FileCache>    buffer;
        boost::recursive_mutex          mutex;
        boost::recursive_mutex          sendMutex;
        boost::condition_variable_any   cond;
        std::string                     fileName;
        struct fuse_file_info           ffi;
        bool                            opPending;

        WriteCache()
          : opPending(false) 
        {
        }
    };

    struct ReadCache {
        boost::shared_ptr<FileCache>    buffer;
        boost::recursive_mutex          mutex;
        boost::condition_variable_any   cond;
        std::string                     fileName;
        struct fuse_file_info           ffi;
        size_t                          blockSize;
        int                             openCount;
        boost::unordered_map <fd_type, off_t> lastBlock;
        off_t                           endOfFile;

        ReadCache()
          : blockSize(4096),
            openCount(0),
            endOfFile(0)
        {
        }
    };

    struct PrefetchJob {
        std::string     fileName;
        off_t           offset;
        size_t          size;
        fd_type         fh;

        PrefetchJob(std::string &fileName, off_t offset, size_t size, fd_type fh) 
          : fileName(fileName),
            offset(offset),
            size(size),
            fh(fh)
        {
        }

        bool operator< (const PrefetchJob &other) 
        {
            return (offset < other.offset) || (offset == other.offset && size < other.size);
        }
    };

    struct PrefetchJobCompare
    {
        bool operator() (const PrefetchJob &a, const PrefetchJob &b) 
        {
            return a.offset < b.offset || (a.offset == b.offset && a.size < b.size);
        }
    };

    typedef boost::shared_ptr<WriteCache> write_buffer_ptr;
    typedef boost::shared_ptr<ReadCache> read_buffer_ptr;

    typedef std::map<uint64_t, write_buffer_ptr> write_cache_map_t;
    typedef std::map<std::string, read_buffer_ptr> read_cache_map_t;

    BufferAgent(write_fun, read_fun);
    virtual ~BufferAgent();

    virtual int onWrite(std::string path, const std::string &buf, size_t size, off_t offset, ffi_type);
    virtual int onRead(std::string path, std::string &buf, size_t size, off_t offset, ffi_type);
    virtual int onFlush(std::string path, ffi_type);
    virtual int onRelease(std::string path, ffi_type);
    virtual int onOpen(std::string path, ffi_type);

    virtual void agentStart(int worker_count = 5);
    virtual void agentStop();

private:

    volatile bool                           m_agentActive;
    std::vector<boost::shared_ptr<boost::thread> >          m_workers;

    boost::recursive_mutex                  m_wrMutex;
    boost::condition_variable_any           m_wrCond;
    write_cache_map_t                       m_wrCacheMap;
    std::list<fd_type>                      m_wrJobQueue;

    boost::recursive_mutex                  m_rdMutex;
    boost::condition_variable_any           m_rdCond;
    read_cache_map_t                        m_rdCacheMap;
    std::multiset<PrefetchJob, PrefetchJobCompare> m_rdJobQueue;

    write_fun                               doWrite;
    read_fun                                doRead;
    
    virtual void writerLoop();
    virtual void readerLoop();

    virtual boost::shared_ptr<FileCache> newFileCache(bool isBuffer = true);
};


} // namespace helpers 
} // namespace veil

