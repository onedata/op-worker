#include "fileCache.h"
#include "glog/logging.h"
#include "helpers/storageHelperFactory.h"

#define MAX_BLOCK_SIZE 10

namespace veil {
namespace helpers {

using namespace std;

FileCache::FileCache(uint32_t blockSize, bool isBuffer) 
  : m_isBuffer(isBuffer),
    m_curBlockNo(1),
    m_blockSize(blockSize),
    m_byteSize(0)
{
}

FileCache::~FileCache() 
{
}

bool FileCache::readData(off_t offset, size_t size, std::string &buff)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);

    discardExpired();
    buff.resize(0);

    multiset<block_ptr>::const_iterator it = m_fileBlocks.upper_bound(block_ptr(new FileBlock(offset)));
    
    if(it == m_fileBlocks.begin())
        return false;

    for(--it; it != m_fileBlocks.end() && buff.size() < size; ++it) 
    {
        if((*it)->offset > offset + buff.size())
            break;

        off_t startFrom = offset + buff.size() - (*it)->offset;
        if(startFrom < (*it)->size()) {
            buff += (*it)->data.substr( startFrom ,  min( (size_t)size - buff.size(), (size_t)startFrom - (*it)->size() ));
        }
    }

    return buff.size() > 0;
}

bool FileCache::writeData(off_t offset, const std::string &buff)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);

    discardExpired();

    FileBlock block(offset, buff);

    return insertBlock(block);
}

void FileCache::debugPrint()
{
    cout << "BlockList:" << endl;
    multiset<block_ptr>::iterator it = m_fileBlocks.begin();
    while(it != m_fileBlocks.end())
    {
        printf("\t Offset: %d, Size: %d, Data: %s, valid_to: %lld\n", (*it)->offset, (*it)->size(), (*it)->data.c_str(), (*it)->valid_to);
        ++it;
    }
}


block_ptr FileCache::removeOldestBlock() 
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);

    if(m_blockExpire.empty())
        return block_ptr();

    discardExpired();

    block_ptr tmp = *m_blockExpire.begin();
    m_blockExpire.erase(m_blockExpire.begin());
    m_fileBlocks.erase(tmp);

    // Update current byte size
    m_byteSize -= tmp->size();

    return tmp;
}

bool FileCache::insertBlock(const FileBlock &block)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);

    discardExpired();

    string cBuff = block.data;
    off_t offset = block.offset;

    // repeat while the is still data to save
    do 
    {
        multiset<block_ptr>::iterator it = m_fileBlocks.upper_bound(block_ptr(new FileBlock(offset)));
        multiset<block_ptr>::iterator next;
        if(it != m_fileBlocks.begin())
            --it;

        while(it != m_fileBlocks.end() && cBuff.size() > 0)
        {
            next = it;
            if((*it)->offset <= offset)
                ++next;

            // At least one byte starting with 'offset' of the block (but not all of them) does not overlap others
            if( ((*it)->offset + (*it)->size() <= offset) || ((*it)->offset > offset) )
            {
                size_t tmpSize = next == m_fileBlocks.end() ? cBuff.size() : min((size_t)((*next)->offset - offset ), cBuff.size());
                block_ptr tmp = block_ptr(new FileBlock(offset, cBuff.substr(0, tmpSize)));

                cBuff = cBuff.substr(tmpSize);
                offset += tmpSize;

                forceInsertBlock(tmp, next);
                break;
            }
            // At least one byte (starting with 'offset') overlaps with older blocks
            else if ( (*it)->offset <= offset && (*it)->offset + (*it)->size() > offset) 
            {
                off_t tStart = offset - (*it)->offset;
                size_t toCpy = min( (size_t)((*it)->offset + (*it)->size() - offset), cBuff.size() );
                
                (*it)->data.replace(tStart, toCpy, cBuff.substr(0, toCpy));
                cBuff = cBuff.substr(toCpy);
                offset += toCpy;
            }

            ++it;
        }

        // Whole current data block does not overlap with older ones
        if(it == m_fileBlocks.end()) 
        {
            block_ptr tmp = block_ptr(new FileBlock(offset, cBuff));
            forceInsertBlock(tmp, it);
            
            cBuff = "";
        }

    } while (cBuff.size() > 0);

    return true;
}

void FileCache::forceInsertBlock(block_ptr block, std::multiset<block_ptr>::iterator whereTo) 
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);

    // This method asumes that given block does not overlap at all with others
    // This is where block merging thakes place 

    if(block->size() == 0)
        return;

    // Set expiration time if it isn't set yet
    if(block->valid_to == 0)
        block->valid_to = m_isBuffer ? ++m_curBlockNo : utils::mtime<uint64_t>() + 5000;
    
    multiset<block_ptr>::iterator it, next, origNext, tmp;
    it = next = origNext = m_fileBlocks.insert(whereTo, block);
    m_blockExpire.insert(block);
    m_byteSize += block->size();

    // If it was first block, there is nothing to megre
    if(m_blockExpire.size() == 1)
        return;

    if(it != m_fileBlocks.begin()) {
        --it;
    } else {
        ++next;
    }

    ++origNext;

    // Iterate from block before the given until merging is done
    do {
        if((*it)->offset + (*it)->size() == (*next)->offset && (*it)->size() < m_blockSize)
        {
            size_t toCpy = min(m_blockSize - (*it)->size(), (*next)->size());
            (*it)->data += (*next)->data.substr(0, toCpy);
            (*next)->data = (*next)->data.substr(toCpy);
            (*next)->offset += toCpy;

            // If merge process generates empty block, delete it
            if((*next)->size() == 0) 
            {
                m_blockExpire.erase(*next);
                tmp = next, tmp++;
                m_fileBlocks.erase(next);
                next = tmp;
            }
        } else if(next != origNext) {
            break;
        } else {
            ++it, ++next;
        }

    } while(it != m_fileBlocks.end() && next != m_fileBlocks.end());
}

size_t FileCache::byteSize()
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);
    return m_byteSize;
}

size_t FileCache::blockCount()
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);
    return m_fileBlocks.size();
}


void FileCache::discardExpired()
{
    if(m_isBuffer) 
        return;

    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);
    while(!m_blockExpire.empty()) 
    {
        block_ptr tmp = *m_blockExpire.begin();

        if(tmp->valid_to > utils::mtime<uint64_t>())
            break;

        m_blockExpire.erase(tmp);
        m_fileBlocks.erase(tmp);
        
        m_byteSize -= tmp->size();

    }
}


} // namespace helpers 
} // namespace veil
