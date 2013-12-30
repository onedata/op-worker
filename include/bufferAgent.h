#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <string>
#include <fuse.h>
#include <unistd.h>
#include <fcntl.h>

#include "fileCache.h"

typedef struct fuse_file_info*  ffi_type;
typedef uint64_t                fd_type;

typedef boost::function<int(std::string path, const std::string &buf, size_t, off_t, ffi_type)>    write_fun;
typedef boost::function<int(std::string path, std::string &buf, size_t, off_t, ffi_type)>          read_fun;

typedef boost::unique_lock<boost::recursive_mutex> unique_lock;


namespace veil {
namespace helpers {

class BufferAgent
{
public:

    struct LockableCache {
        boost::shared_ptr<FileCache>    buffer;
        boost::recursive_mutex          mutex;
        boost::condition_variable_any   cond;
        std::string                     fileName;
        struct fuse_file_info           ffi;
    };

    typedef boost::shared_ptr<LockableCache> buffer_ptr;

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

    volatile bool                       m_agentActive;
    boost::recursive_mutex                        m_loopMutex;
    boost::condition_variable_any           m_loopCond;
    std::vector<boost::shared_ptr<boost::thread> >          m_workers;
    std::map<fd_type, buffer_ptr>       m_cacheMap;
    std::list<fd_type>                  m_jobQueue;

    write_fun                           doWrite;
    read_fun                            doRead;
    
    virtual void workerLoop();
    virtual boost::shared_ptr<FileCache> newFileCache();
};


} // namespace helpers 
} // namespace veil

