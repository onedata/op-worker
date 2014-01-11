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

// Convinience typedef
typedef struct fuse_file_info*  ffi_type;
typedef uint64_t                fd_type;
typedef boost::unique_lock<boost::recursive_mutex> unique_lock;
typedef boost::unordered_map<fd_type, size_t>       wrbuf_size_mem_t;
typedef boost::unordered_map<std::string, size_t>   rdbuf_size_mem_t;


// Typedef for original read / write functions
typedef boost::function<int(std::string path, const std::string &buf, size_t, off_t, ffi_type)>    write_fun;
typedef boost::function<int(std::string path, std::string &buf, size_t, off_t, ffi_type)>          read_fun;

/**
 * BufferAgent gives replacement methods for read and write operations that acts as proxy for real ones.
 * The BufferAgent decides if and when, real read/write operation will be used. 
 */
class BufferAgent
{
public:

    /// State holder for write operations for the file 
    struct WriteCache {
        boost::shared_ptr<FileCache>    buffer;
        boost::recursive_mutex          mutex;    
        boost::recursive_mutex          sendMutex;
        boost::condition_variable_any   cond;
        std::string                     fileName;
        struct fuse_file_info           ffi;
        bool                            opPending;
        int                             lastError;

        WriteCache()
          : opPending(false),
            lastError(0)
        {
        }
    };

    /// State holder for read operations for the file 
    struct ReadCache {
        boost::shared_ptr<FileCache>    buffer;
        boost::recursive_mutex          mutex;
        boost::condition_variable_any   cond;
        std::string                     fileName;
        struct fuse_file_info           ffi;
        size_t                          blockSize;      // Current prefered block size
        int                             openCount;      // How many file descriptors is opened to the file atm
        boost::unordered_map <fd_type, off_t> lastBlock;// Last requested block (per file descriptor)
        off_t                           endOfFile;      // Last detected end of file (its offset)

        ReadCache()
          : blockSize(4096),
            openCount(0),
            endOfFile(0)
        {
        }
    };

    /// Internal type of Prefetching workers' job
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

    /// Comparator for PrefetchJob struct that ordes them by block offset that they refer to
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

    /// onWrite shall be called on each write operation that filesystem user requests
    virtual int onWrite(std::string path, const std::string &buf, size_t size, off_t offset, ffi_type);
    
    /// onRead shall be called on each read operation that filesystem user requests
    virtual int onRead(std::string path, std::string &buf, size_t size, off_t offset, ffi_type);
    
    /// onFlush shall be called on each flush operation that filesystem user requests
    virtual int onFlush(std::string path, ffi_type);
    
    /// onRelease shall be called on each release operation that filesystem user requests
    virtual int onRelease(std::string path, ffi_type);
    
    /// onOpen shall be called on each open operation that filesystem user requests
    virtual int onOpen(std::string path, ffi_type);

    /// Starts BufferAgent worker threads
    virtual void agentStart(int worker_count = 5);

    /// Stops BufferAgent worker threads
    virtual void agentStop();

private:

    volatile bool                                   m_agentActive;
    std::vector<boost::shared_ptr<boost::thread> >  m_workers;     /// Worker threads list

    // State holders and job queues for write operations
    boost::recursive_mutex                  m_wrMutex;
    boost::condition_variable_any           m_wrCond;
    write_cache_map_t                       m_wrCacheMap;
    std::list<fd_type>                      m_wrJobQueue;

    // State holders and job queues for read operations
    boost::recursive_mutex                  m_rdMutex;
    boost::condition_variable_any           m_rdCond;
    read_cache_map_t                        m_rdCacheMap;
    std::multiset<PrefetchJob, PrefetchJobCompare> m_rdJobQueue;

    /// Real write function pointer
    write_fun                               doWrite;

    /// Real read function pointer
    read_fun                                doRead;
    
    virtual void writerLoop();
    virtual void readerLoop();

    /// Instantiate FileCache class. Useful in tests for mocking
    virtual boost::shared_ptr<FileCache> newFileCache(bool isBuffer = true);


    // Memory management. Memory current state.
    static boost::recursive_mutex           m_bufferSizeMutex;

    volatile static size_t              m_rdBufferTotalSize;
    volatile static size_t              m_wrBufferTotalSize;
    static rdbuf_size_mem_t             m_rdBufferSizeMem;
    static wrbuf_size_mem_t             m_wrBufferSizeMem;

    static void updateWrBufferSize(fd_type, size_t);
    static void updateRdBufferSize(std::string, size_t);
    static size_t getWriteBufferSize();
    static size_t getReadBufferSize();
};


} // namespace helpers 
} // namespace veil

