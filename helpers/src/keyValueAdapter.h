/**
 * @file keyValueAdapter.h
 * @author Krzysztof Trzepla
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_KEY_VALUE_ADAPTER_H
#define HELPERS_KEY_VALUE_ADAPTER_H

#include "helpers/IStorageHelper.h"

#include <asio.hpp>
#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace one {
namespace helpers {

constexpr std::size_t DEFAULT_BLOCK_SIZE = 5 * 1024 * 1024;

class KeyValueHelper;

/**
 * The @c KeyValueAdapter class translates POSIX operations to operations
 * available on key-value storage by splitting consistent range of bytes into
 * blocks.
 */
class KeyValueAdapter : public IStorageHelper {
public:
    using Locks = tbb::concurrent_hash_map<std::string, bool>;

    /**
     * Constructor.
     * @param helper @c KeyValueHelper instance that provides low level storage
     * access.
     * @param service IO service used for asynchronous operations.
     * @param locks Map of locks used to exclude concurrent operations on the
     * same storage block.
     * @param blockSize Size of storage block.
     */
    KeyValueAdapter(std::unique_ptr<KeyValueHelper> helper,
        asio::io_service &service, Locks &locks,
        std::size_t blockSize = DEFAULT_BLOCK_SIZE);

    virtual CTXPtr createCTX(
        std::unordered_map<std::string, std::string> params) override;

    virtual void ash_unlink(CTXPtr ctx, const boost::filesystem::path &p,
        VoidCallback callback) override;

    virtual void ash_read(CTXPtr ctx, const boost::filesystem::path &p,
        asio::mutable_buffer buf, off_t offset,
        GeneralCallback<asio::mutable_buffer> callback) override;

    virtual void ash_write(CTXPtr ctx, const boost::filesystem::path &p,
        asio::const_buffer buf, off_t offset,
        GeneralCallback<std::size_t> callback) override;

    virtual void ash_truncate(CTXPtr ctx, const boost::filesystem::path &p,
        off_t size, VoidCallback callback) override;

    virtual void ash_mknod(CTXPtr ctx, const boost::filesystem::path &p,
        mode_t mode, FlagsSet flags, dev_t rdev, VoidCallback callback) override
    {
        callback(SUCCESS_CODE);
    }

    virtual void ash_mkdir(CTXPtr ctx, const boost::filesystem::path &p,
        mode_t mode, VoidCallback callback) override
    {
        callback(SUCCESS_CODE);
    }

    virtual void ash_chmod(CTXPtr ctx, const boost::filesystem::path &p,
        mode_t mode, VoidCallback callback) override
    {
        callback(SUCCESS_CODE);
    }

private:
    uint64_t getBlockId(off_t offset);

    off_t getBlockOffset(off_t offset);

    void readBlocks(CTXPtr ctx, std::string prefix, asio::mutable_buffer buf,
        off_t offset, std::size_t fileSize,
        GeneralCallback<asio::mutable_buffer> callback);

    std::size_t readBlock(CTXPtr ctx, const std::string &prefix,
        asio::mutable_buffer buf, std::size_t bufOffset, uint64_t blockId,
        off_t blockOffset);

    asio::mutable_buffer readBlock(
        CTXPtr ctx, std::string key, asio::mutable_buffer buf, off_t offset);

    void writeBlock(CTXPtr ctx, const std::string &prefix,
        asio::const_buffer buf, std::size_t bufOffset, uint64_t blockId,
        off_t blockOffset, std::size_t blockSize);

    void logError(std::string operation, const std::system_error &error);

    std::unique_ptr<KeyValueHelper> m_helper;
    asio::io_service &m_service;
    Locks &m_locks;
    const std::size_t m_blockSize;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_KEY_VALUE_ADAPTER_H
