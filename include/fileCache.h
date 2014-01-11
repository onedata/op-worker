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

/**
 * FileBlock represents single file block of data. 
 * Holds offset, size, expiration time and data itself.
 */
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

// Convinience typedef to FileBlock struct
typedef boost::shared_ptr<FileBlock>    block_ptr;
typedef boost::weak_ptr<FileBlock>      block_weak_ptr;

/// Comparator for pointers to FileBlock which orders them by expiration date
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

/// Comparator for pointers to FileBlock which orders them by data offset
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

/**
 * FileCache object represents single file memory cache.
 * Cache allows to write and read data. Cached file is meant to be incomplete.
 * Also, FileCache handles blocks' merge.
 */
class FileCache
{
public:

    /**
     * The constructor.
     * @param blockSize defines maximum block size that resault from blocks' merge.
     * @param isBuffer defines is this FileCache will be used as write buffer, which means that expire data is not used.
     */
    FileCache(uint32_t blockSize, bool isBuffer = true); 
    virtual ~FileCache();

    virtual bool        readData(off_t, size_t, std::string &buff); ///< Reads data from buffer
                                                                    ///< @return true if at least 1 byte cound be read
    virtual bool        writeData(off_t, const std::string &buff);  ///< Writes data to buffer
    virtual block_ptr   removeOldestBlock();                        ///< Returns pointer to oldest added FileBlock and removes it from the Cache.
    virtual bool        insertBlock(const FileBlock&);              ///< Inserts givent FileBlock into the Cache 

    virtual size_t      byteSize();                                 ///< Returns hoav many bytes are currently cached.
    virtual size_t      blockCount();                               ///< Returns block count for the Cache 

    virtual void        debugPrint();                               ///< Prints all FileBlocks to stdout. 
                                                                    ///< Usefull for debugging


private:
    bool                                            m_isBuffer;
    uint64_t                                        m_curBlockNo;
    uint32_t                                        m_blockSize;
    boost::recursive_mutex                          m_fileBlocksMutex;
    std::multiset<block_ptr, _OrderByOffset>        m_fileBlocks;
    size_t                                          m_byteSize;

    std::multiset<block_ptr, _OrderByValidTo> m_blockExpire;

    virtual void discardExpired();  ///< Remove expired blocks from teh Cache 
    virtual void forceInsertBlock(block_ptr block, std::multiset<block_ptr>::iterator whereTo);
};

} // namespace helpers 
} // namespace veil

