/**
 * @file s3Helper.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "s3Helper.h"
#include "logging.h"

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <boost/algorithm/string.hpp>
#include <glog/stl_logging.h>

#include <cstring>

namespace one {
namespace helpers {

S3Helper::S3Helper(std::unordered_map<std::string, std::string> args)
    : m_args{std::move(args)}
{
}

CTXPtr S3Helper::createCTX(std::unordered_map<std::string, std::string> params)
{
    return std::make_shared<S3HelperCTX>(std::move(params), m_args);
}

asio::mutable_buffer S3Helper::getObject(
    CTXPtr rawCTX, std::string key, asio::mutable_buffer buf, off_t offset)
{
    auto ctx = getCTX(std::move(rawCTX));
    auto size = asio::buffer_size(buf);
    auto data = asio::buffer_cast<char *>(buf);

    Aws::S3::Model::GetObjectRequest request{};
    request.SetBucket(ctx->getBucket());
    request.SetKey(key);
    request.SetRange(
        rangeToString(offset, static_cast<off_t>(offset + size - 1)));
    request.SetResponseStreamFactory([&]() {
        auto stream = new std::stringstream{};
        stream->rdbuf()->pubsetbuf(data, size);
        return stream;
    });

    auto outcome = ctx->getClient()->GetObject(request);
    auto code = getReturnCode(outcome);
    if (code != SUCCESS_CODE) {
        std::memset(data, 0, size);
        throwOnError("GetObject", outcome);
    }

    return asio::buffer(
        buf, static_cast<std::size_t>(outcome.GetResult().GetContentLength()));
}

off_t S3Helper::getObjectsSize(
    CTXPtr rawCTX, const std::string &prefix, std::size_t objectSize)
{
    auto ctx = getCTX(std::move(rawCTX));

    Aws::S3::Model::ListObjectsRequest request{};
    request.SetBucket(ctx->getBucket());
    request.SetPrefix(adjustPrefix(std::move(prefix)));
    request.SetDelimiter(OBJECT_DELIMITER);
    request.SetMaxKeys(1);

    auto outcome = ctx->getClient()->ListObjects(request);
    throwOnError("ListObjects", outcome);

    if (outcome.GetResult().GetContents().empty())
        return 0;

    auto key = outcome.GetResult().GetContents().back().GetKey();

    return getObjectId(std::move(key)) * objectSize +
        outcome.GetResult().GetContents().back().GetSize();
}

std::size_t S3Helper::putObject(
    CTXPtr rawCTX, std::string key, asio::const_buffer buf)
{
    auto ctx = getCTX(std::move(rawCTX));

    Aws::S3::Model::PutObjectRequest request{};
    auto size = asio::buffer_size(buf);
    auto stream = std::make_shared<std::stringstream>();
    stream->rdbuf()->pubsetbuf(
        const_cast<char *>(asio::buffer_cast<const char *>(buf)), size);
    request.SetBucket(ctx->getBucket());
    request.SetKey(key);
    request.SetContentLength(size);
    request.SetBody(stream);

    auto outcome = ctx->getClient()->PutObject(request);
    throwOnError("PutObject", outcome);

    return size;
}

void S3Helper::deleteObjects(CTXPtr rawCTX, std::vector<std::string> keys)
{
    auto ctx = getCTX(std::move(rawCTX));

    Aws::S3::Model::DeleteObjectsRequest request{};
    request.SetBucket(ctx->getBucket());

    while (!keys.empty()) {
        Aws::S3::Model::Delete container;

        for (auto s = std::min(keys.size(), MAX_DELETE_OBJECTS); s > 0; --s) {
            container.AddObjects(Aws::S3::Model::ObjectIdentifier{}.WithKey(
                std::move(keys.back())));
            keys.pop_back();
        }

        request.SetDelete(std::move(container));
        auto outcome = ctx->getClient()->DeleteObjects(request);
        throwOnError("DeleteObjects", outcome);
    }
}

std::vector<std::string> S3Helper::listObjects(
    CTXPtr rawCTX, std::string prefix)
{
    auto ctx = getCTX(std::move(rawCTX));

    Aws::S3::Model::ListObjectsRequest request{};
    request.SetBucket(ctx->getBucket());
    request.SetPrefix(adjustPrefix(std::move(prefix)));
    request.SetDelimiter(OBJECT_DELIMITER);

    std::vector<std::string> keys;

    while (true) {
        auto outcome = ctx->getClient()->ListObjects(request);
        throwOnError("ListObjects", outcome);

        for (const auto &object : outcome.GetResult().GetContents())
            keys.emplace_back(object.GetKey());

        if (!outcome.GetResult().GetIsTruncated())
            return keys;

        request.SetMarker(outcome.GetResult().GetNextMarker());
    }
}

std::shared_ptr<S3HelperCTX> S3Helper::getCTX(CTXPtr rawCTX) const
{
    auto ctx = std::dynamic_pointer_cast<S3HelperCTX>(rawCTX);
    if (ctx == nullptr) {
        LOG(INFO) << "Helper changed. Creating new context with arguments: "
                  << m_args;
        return std::make_shared<S3HelperCTX>(rawCTX->parameters(), m_args);
    }
    return ctx;
}

S3HelperCTX::S3HelperCTX(std::unordered_map<std::string, std::string> params,
    std::unordered_map<std::string, std::string> args)
    : IStorageHelperCTX{std::move(params)}
    , m_args{std::move(args)}
{
    m_args.insert({S3_HELPER_ACCESS_KEY_ARG, ""});
    m_args.insert({S3_HELPER_SECRET_KEY_ARG, ""});
    init();
}

void S3HelperCTX::setUserCTX(std::unordered_map<std::string, std::string> args)
{
    m_args.swap(args);
    m_args.insert(args.begin(), args.end());
    init();
}

std::unordered_map<std::string, std::string> S3HelperCTX::getUserCTX()
{
    return {{S3_HELPER_ACCESS_KEY_ARG, m_args.at(S3_HELPER_ACCESS_KEY_ARG)},
        {S3_HELPER_SECRET_KEY_ARG, m_args.at(S3_HELPER_SECRET_KEY_ARG)}};
}

const std::string &S3HelperCTX::getBucket() const
{
    return m_args.at(S3_HELPER_BUCKET_NAME_ARG);
}

const std::unique_ptr<Aws::S3::S3Client> &S3HelperCTX::getClient() const
{
    return m_client;
}

void S3HelperCTX::init()
{
    static S3HelperApiInit init;

    Aws::Auth::AWSCredentials credentials{m_args.at(S3_HELPER_ACCESS_KEY_ARG),
        m_args.at(S3_HELPER_SECRET_KEY_ARG)};
    Aws::Client::ClientConfiguration configuration;

    auto search = m_args.find(S3_HELPER_SCHEME_ARG);
    if (search != m_args.end() && boost::iequals(search->second, "http"))
        configuration.scheme = Aws::Http::Scheme::HTTP;

    search = m_args.find(S3_HELPER_HOST_NAME_ARG);
    if (search != m_args.end())
        configuration.endpointOverride = search->second;

    m_client = std::make_unique<Aws::S3::S3Client>(credentials, configuration);
}

std::map<Aws::S3::S3Errors, std::errc> S3Helper::s_errors = {
    {Aws::S3::S3Errors::INVALID_PARAMETER_VALUE, std::errc::invalid_argument},
    {Aws::S3::S3Errors::MISSING_ACTION, std::errc::not_supported},
    {Aws::S3::S3Errors::SERVICE_UNAVAILABLE, std::errc::host_unreachable},
    {Aws::S3::S3Errors::NETWORK_CONNECTION, std::errc::network_unreachable},
    {Aws::S3::S3Errors::REQUEST_EXPIRED, std::errc::timed_out},
    {Aws::S3::S3Errors::ACCESS_DENIED, std::errc::permission_denied},
    {Aws::S3::S3Errors::UNKNOWN, std::errc::no_such_file_or_directory},
    {Aws::S3::S3Errors::NO_SUCH_BUCKET, std::errc::no_such_file_or_directory},
    {Aws::S3::S3Errors::NO_SUCH_KEY, std::errc::no_such_file_or_directory},
    {Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
        std::errc::no_such_file_or_directory}};

} // namespace helpers
} // namespace one
