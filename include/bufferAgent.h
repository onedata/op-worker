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
 * The BufferAgent decides if and when, real read/write operation will be used in order to improve theirs preformance.
 * For write operations BufferAgent will accumulate date to increase block size in for write operations.
 * For read calls, BufferAgent will try to prefetch data before they are actually needed.
 * In order to use BufferAgent, open, release, write, read and flush methods of Storage Helper shall be replaced with the class 
 * onOpen, onRelease, onWrite, onRead and onFlush methods respectively. Also real Storage helpers callback shall be provided with the
 * BufferAgent constructor.
 */
class BufferAgent
{
public:

    /// State holder for write operations for the file 
    struct WriteCache {
        boost::shared_ptr<FileCache>    buffer;     ///< Actual buffer object.
        boost::recursive_mutex          mutex;    
        boost::recursive_mutex          sendMutex;
        boost::condition_variable_any   cond;
        std::string                     fileName;
        struct fuse_file_info           ffi;         ///< Saved fuse_file_info struct that we need to pass for each storage helpers' call.
        bool                            opPending;
        int                             lastError;  ///< Last write error

        WriteCache()
          : opPending(false),
            lastError(0)
        {
        }
    };

    /// State holder for read operations for the file 
    struct ReadCache {
        boost::shared_ptr<FileCache>    buffer;         ///< Actual buffer object.
        boost::recursive_mutex          mutex;
        boost::condition_variable_any   cond;
        std::string                     fileName;
        struct fuse_file_info           ffi;            ///< Saved fuse_file_info struct that we need to pass for each storage helpers' call.
        size_t                          blockSize;      ///< Current prefered block size
        int                             openCount;      ///< How many file descriptors is opened to the file atm
        boost::unordered_map <fd_type, off_t> lastBlock;///< Last requested block (per file descriptor)
        off_t                           endOfFile;      ///< Last detected end of file (its offset)

        ReadCache()
          : blockSize(4096),
            openCount(0),
            endOfFile(0)
        {
        }
    };

    /// Internal type of Prefetching workers' job
    /// Every time that BufferAgent thinks data prefetch is needed, the PrefetchJob
    /// object end up in worker threads' job queue 
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

        /// Orders PrefetchJob by its offset and size.
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

    /**
     * BufferAgent constructor.
     * @param write_fun Write function that writes data to filesystem. This shall be storage helpers' write callback. See write_fun type for signature.
     * @param read_fun Read function that provides filesystems' data. This shall be storage helpers' read callback. See read_fun type for signature.
     */
    BufferAgent(write_fun, read_fun);
    virtual ~BufferAgent();

    /// onWrite shall be called on each write operation that filesystem user requests - accumulates data while sending it asynchronously.
    virtual int onWrite(std::string path, const std::string &buf, size_t size, off_t offset, ffi_type);
    
    /// onRead shall be called on each read operation that filesystem user requests.
    /// onRead returns buffered data if available.
    virtual int onRead(std::string path, std::string &buf, size_t size, off_t offset, ffi_type);
    
    /// onFlush shall be called on each flush operation that filesystem user requests.
    /// This metod flushes all buffered data.
    virtual int onFlush(std::string path, ffi_type);
    
    /// onRelease shall be called on each release operation that filesystem user requests
    virtual int onRelease(std::string path, ffi_type);
    
    /// onOpen shall be called on each open operation that filesystem user requests
    virtual int onOpen(std::string path, ffi_type);

    /// Starts BufferAgent worker threads
    /// @param worker_count How many worker threads shall be stared to process prefetch request and send buffored data.
    virtual void agentStart(int worker_count = 5);

    /// Stops BufferAgent worker threads
    virtual void agentStop();

private:

    volatile bool                                   m_agentActive; ///< Status of worker threads. Setting this to false exits workers' main loop.
    std::vector<boost::shared_ptr<boost::thread> >  m_workers;     ///< Worker threads list

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

    /// Real write function pointer (storage helpers' write method)
    write_fun                               doWrite;

    /// Real read function pointer (storage helpers' read method)
    read_fun                                doRead;
    
    virtual void writerLoop();  ///< Main loop for worker thread that sends buffored data.
    virtual void readerLoop();  ///< Main loop for worker thread that prefetches data.

    /// Instantiate FileCache class. Useful in tests for mocking
    /// @param isBuffer Set this argument to false if data in buffer shall be automatically cleared after same short time.
    virtual boost::shared_ptr<FileCache> newFileCache(bool isBuffer = true);


    // Memory management. Memory current state update/check.
    static boost::recursive_mutex           m_bufferSizeMutex;

    volatile static size_t              m_rdBufferTotalSize;    ///< Current total size of prefetched data.
    volatile static size_t              m_wrBufferTotalSize;    ///< Current total size of buffored data.
    static rdbuf_size_mem_t             m_rdBufferSizeMem;      ///< Current sizes of prefetched data for each file individually.
    static wrbuf_size_mem_t             m_wrBufferSizeMem;      ///< Current sizes of buffored data for each file individually.

    static void updateWrBufferSize(fd_type, size_t size);       ///< Updates size of buffored data for the specified file.
                                                                ///< @param size Shall be set to current buffer size.
    static void updateRdBufferSize(std::string, size_t size);   ///< Updates size of prefetched data for the specified file.
                                                                ///< @param size Shall be set to current buffer size.
    
    static size_t getWriteBufferSize();                         ///< Returns current total size of buffored data.
    static size_t getReadBufferSize();                          ///< Returns current total size of prefetched data.
};


} // namespace helpers 
} // namespace veil

