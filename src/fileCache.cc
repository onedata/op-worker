#include "fileCache.h"
#include "glog/logging.h"
#include "helpers/storageHelperFactory.h"

#define MAX_BLOCK_SIZE 10

namespace veil {
namespace helpers {

using namespace std;

FileCache::FileCache(uint32_t blockSize, bool isBuffer) 
  : m_byteSize(0),
    m_isBuffer(isBuffer),
    m_curBlockNo(1),
    m_blockSize(blockSize)
{
}

FileCache::~FileCache() 
{
}

bool FileCache::readData(off_t offset, size_t size, std::string &buff)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);
    debugPrint();
    discardExpired();
    buff.resize(0);

    list<block_ptr>::const_iterator it = upper_bound(m_fileBlocks.begin(), m_fileBlocks.end(), offset, OrderByOffset);
    
    if(it == m_fileBlocks.begin())
        return false;

    for(--it; it != m_fileBlocks.end() && buff.size() < size; ++it) 
    {
        if((*it)->offset > offset + buff.size())
            break;

        off_t startFrom = offset + buff.size() - (*it)->offset;
        if(startFrom < (*it)->size) {
            buff += (*it)->data.substr( startFrom ,  min( (size_t)size - buff.size(), (size_t)startFrom - (*it)->size ));
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
    return;
    cout << "BlockList:" << endl;
    list<block_ptr>::iterator it = m_fileBlocks.begin();
    while(it != m_fileBlocks.end())
    {
        printf("\t Offset: %d, Size: %d, Data: %s, valid_to: %lld\n", (*it)->offset, (*it)->size, (*it)->data.c_str(), (*it)->valid_to);
        ++it;
    }
}


block_ptr FileCache::removeOldestBlock() 
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);

    if(m_blockExpire.empty())
        return block_ptr();

    discardExpired();

    // cout << "Queue: " << m_blockExpire.size() << endl;

    block_ptr tmp = *m_blockExpire.begin();
    m_blockExpire.erase(m_blockExpire.begin());
    m_fileBlocks.remove(tmp);

    m_byteSize -= tmp->size;

    return tmp;
}

bool FileCache::insertBlock(const FileBlock &block)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);

    discardExpired();

    string cBuff = block.data;
    off_t offset = block.offset;

    do 
    {
        // cout << "do..." << endl;
        list<block_ptr>::iterator it = upper_bound(m_fileBlocks.begin(), m_fileBlocks.end(), offset, OrderByOffset);
        list<block_ptr>::iterator next;
        // cout << "UpperBound: " << offset << " " << cBuff << endl;
        if(it != m_fileBlocks.begin())
            --it;

        while(it != m_fileBlocks.end() && cBuff.size() > 0)
        {
            next = it;
            if((*it)->offset <= offset)
                ++next;

            // cout << "FileCache: dbg_in: offset(" << offset << "), (*it)->offset(" << (*it)->offset << "), (*next)->offset(" << ( next != m_fileBlocks.end() ? (*next)->offset : -1) << ")" << endl;
                

            if( ((*it)->offset < offset && (*it)->offset + (*it)->size <= offset) || ((*it)->offset > offset) )
            {
                size_t tmpSize = next == m_fileBlocks.end() ? cBuff.size() : min((size_t)((*next)->offset - offset ), cBuff.size());
                block_ptr tmp = block_ptr(new FileBlock(offset, cBuff.substr(0, tmpSize)));
                // cout << "FileCache: ?: offset(" << offset << "), size(" << tmpSize << "), buff(" << cBuff.substr(0, tmpSize) << ")" << endl;
                // cout << "FileCache: dbg: offset(" << offset << "), (*it)->offset(" << (*it)->offset << "), (*next)->offset(" << ( next != m_fileBlocks.end() ? (*next)->offset : -1)<< ")" << endl;
                
                // cout << cBuff.size() << endl;
                cBuff = cBuff.substr(tmpSize);
                offset += tmpSize;

                forceInsertBlock(tmp, next);

                break;
            }
            else if ( (*it)->offset <= offset && (*it)->offset + (*it)->size > offset) 
            {
                off_t tStart = offset - (*it)->offset;
                off_t sStart = 0;
                size_t toCpy = min( (size_t)((*it)->offset + (*it)->size - offset), cBuff.size() );
                // cout << "FileCache: override: offset(" << offset << "), size(" << toCpy << "), buff(" << cBuff.substr(0, toCpy) << ")" << endl;
                
                (*it)->data.replace(tStart, toCpy, cBuff.substr(0, toCpy));
                cBuff = cBuff.substr(toCpy);
                offset += toCpy;

                //continue;
            }

            ++it;
        }

        if(it == m_fileBlocks.end()) 
        {
            // cout << "it is at the end" << endl;
            block_ptr tmp = block_ptr(new FileBlock(offset, cBuff));
            forceInsertBlock(tmp, it);
            
            // cout << "FileCache: push_back: offset(" << offset << "), size(" << cBuff.size() << "), buff(" << cBuff << ")" << endl;
            cBuff = "";

            
        }

    } while (cBuff.size() > 0);

    return true;
}

void FileCache::forceInsertBlock(block_ptr block, std::list<block_ptr>::iterator whereTo) 
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);

    if(block->valid_to == 0)
        block->valid_to = m_isBuffer ? ++m_curBlockNo : utils::mtime<uint64_t>() + 1000;
    
    list<block_ptr>::iterator it, next, origNext;
    it = next = origNext = m_fileBlocks.insert(whereTo, block);
    m_blockExpire.insert(block);
    m_byteSize += block->size;

    // cout << "Insert: " << block->data << endl;
    debugPrint();

    if(m_blockExpire.size() == 1)
        return;

    if(it != m_fileBlocks.begin()){
        --it;
    } else {
        ++next;
    }

    ++origNext;

    do {
        if((*it)->offset + (*it)->size == (*next)->offset && (*it)->size < m_blockSize)
        {
            size_t toCpy = min(m_blockSize - (*it)->size, (*next)->size);
            (*it)->data += (*next)->data.substr(0, toCpy);
            (*next)->data = (*next)->data.substr(toCpy);
            (*next)->size = (*next)->data.size();
            (*next)->offset += toCpy;
            (*it)->size += toCpy;

            if((*next)->size == 0) {
                m_blockExpire.erase(*next);
                next = m_fileBlocks.erase(next);

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


void FileCache::discardExpired(bool rebuildQueue)
{
    boost::unique_lock<boost::recursive_mutex> guard(m_fileBlocksMutex);

    while(!m_blockExpire.empty()) 
    {
        block_ptr tmp;
        if(m_blockExpire.begin() != m_blockExpire.end())
            tmp = *m_blockExpire.begin();
        
        if(tmp && (!m_isBuffer && tmp->valid_to < utils::mtime<uint64_t>() ) ) {
            list<block_ptr>::iterator it = lower_bound(m_fileBlocks.begin(), m_fileBlocks.end(), tmp->offset, OrderByOffset);

            if(it != m_fileBlocks.end() && (*it)->offset <= tmp->offset) {
                if((*it) == tmp) 
                {
                    m_fileBlocks.erase(it);
                    m_byteSize -= tmp->size;
                    break;
                }
                --it;
            }

            m_blockExpire.erase(m_blockExpire.begin());
        } else {
            break;
        }

    }
}


} // namespace helpers 
} // namespace veil
