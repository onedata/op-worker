#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <map>
#include <queue>
#include <string>
#include <list>

#include <fuse.h>
#include <unistd.h>
#include <fcntl.h>
 

namespace veil {
namespace helpers {

struct FileBlock
{
    off_t               offset;
    size_t              _size;
    std::string         data;

    uint64_t            valid_to;

    FileBlock() 
      : _size(0),
        valid_to(0),
        offset(0)
    {
    }

    FileBlock(off_t off) 
      : _size(0),
        valid_to(0),
        offset(off)
    {
    }

    FileBlock(off_t off, const std::string &buff, uint64_t valid = 0) 
      : _size(buff.size()),
        offset(off),
        data(buff),
        valid_to(valid)
    {
    }
 
    size_t size() const
    {
        return data.size();
    }

    bool operator== ( FileBlock const &q) const { return offset == q.offset && size() == q.size(); }
    
};

typedef boost::shared_ptr<FileBlock>    block_ptr;
typedef boost::weak_ptr<FileBlock>      block_weak_ptr;

static struct _OrderByValidTo
{
    bool operator() (block_ptr const &a, block_ptr const &b) 
    { 
        if(!a->valid_to)
            return false;
        if(!b->valid_to)
            return true; 

        return a->valid_to < b->valid_to;
    }
} OrderByValidTo;

static struct _OrderByOffset
{
    bool operator() (block_ptr const &a, block_ptr const &b) 
    {
        return a->offset < b->offset;
    }

    bool operator() (off_t a, block_ptr const &b) 
    { 
        return a < b->offset;
    }

    bool operator() (block_ptr const &b, off_t a) 
    { 
        return a < b->offset;
    }
} OrderByOffset;

class FileCache
{
public:
    FileCache(uint32_t blockSize, bool isBuffer = true);
    virtual ~FileCache();

    virtual bool readData(off_t, size_t, std::string &buff);
    virtual bool writeData(off_t, const std::string &buff);
    virtual block_ptr removeOldestBlock();
    virtual bool insertBlock(const FileBlock&);

    virtual size_t byteSize();
    virtual size_t blockCount();

    virtual void debugPrint();


private:
    bool                                            m_isBuffer;
    uint64_t                                        m_curBlockNo;
    uint32_t                                        m_blockSize;
    boost::recursive_mutex                          m_fileBlocksMutex;
    std::multiset<block_ptr, _OrderByOffset>        m_fileBlocks;
    size_t                                          m_byteSize;

    std::multiset<block_ptr, _OrderByValidTo> m_blockExpire;

    virtual void discardExpired();
    virtual void forceInsertBlock(block_ptr block, std::multiset<block_ptr>::iterator whereTo);
};

} // namespace helpers 
} // namespace veil

