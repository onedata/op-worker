/**
 * @file certificateData.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_CERTIFICATE_DATA_H
#define VEILHELPERS_CERTIFICATE_DATA_H


#include <boost/asio/buffer.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/context_base.hpp>

#include <memory>
#include <string>

namespace veil
{
namespace communication
{

class CertificateData
{
public:
    enum class KeyFormat
    {
        ASN1,
        PEM
    };

    CertificateData(KeyFormat keyFormat);
    virtual ~CertificateData() = default;

    virtual std::shared_ptr<boost::asio::ssl::context>
        initContext(std::shared_ptr<boost::asio::ssl::context> ctx) const = 0;

protected:
    boost::asio::ssl::context_base::file_format keyFormat() const;

private:
    const KeyFormat m_keyFormat;
};

class FilesystemCertificate: public CertificateData
{
public:
    FilesystemCertificate(std::string certPath,
                          std::string keyPath,
                          KeyFormat keyFormat);

    std::shared_ptr<boost::asio::ssl::context>
        initContext(std::shared_ptr<boost::asio::ssl::context> ctx) const;

private:
    const std::string m_certPath;
    const std::string m_keyPath;
};

class InMemoryCertificate: public CertificateData
{
public:
    InMemoryCertificate(boost::asio::const_buffer certData,
                        boost::asio::const_buffer keyData,
                        KeyFormat keyFormat);

    std::shared_ptr<boost::asio::ssl::context>
        initContext(std::shared_ptr<boost::asio::ssl::context> ctx) const;

private:
    const boost::asio::const_buffer m_certData;
    const boost::asio::const_buffer m_keyData;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_CERTIFICATE_DATA_H
