/**
 * @file tls2tcp_cert_aware_session.cpp
 * @author Tomasz Lichon
 * @copyright (C): 2015 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#include "tls2tcp_cert_aware_session.h"

#include "log_message.h"
#include "oneproxy_messages.pb.h"

using std::string;
using namespace std::placeholders;

namespace one {
namespace proxy {

template <typename lambda>
std::shared_ptr<lambda> make_shared_handler(lambda &&l)
{
    return std::make_shared<lambda>(std::forward<lambda>(l));
}

void tls2tcp_cert_aware_session::post_handshake(bool verified)
{
    // Prepare CertInfo
    one::proxy::proto::CertificateInfo certificate_info;
    if (verified) {
        std::array<char, 2048> subject_name;
        X509_NAME_oneline(X509_get_subject_name(peer_cert_),
                subject_name.data(), subject_name.size());
        certificate_info.set_client_subject_dn(subject_name.data());
        certificate_info.set_client_session_id(session_id_);
    }

    // Encode CertInfo
    string certificate_info_data;
    if(!certificate_info.SerializeToString(&certificate_info_data)) {
        LOG(ERROR) << "Cannot serialize certificate info.";
        return;
    }
    auto header = std::make_unique<std::uint32_t>(htonl(certificate_info_data.size()));
    auto msg = std::make_unique<std::string>(std::move(certificate_info_data));

    // Send CertInfo
    std::array<boost::asio::const_buffer, 2> buffers{
            {boost::asio::buffer(static_cast<void *>(&*header), sizeof(*header)),
                    boost::asio::buffer(*msg)}};
    auto handler = make_shared_handler([
            this,
            t = shared_from_this(),
            header = std::move(header), //boost asio needs header and msg in buffer
            msg = std::move(msg),
            verified
    ](const boost::system::error_code & ec)
    {
        if (ec)
            LOG(ERROR) << "Cannot send CertificateInfo message.";
        start_reading(verified);
    });
    boost::asio::async_write(
            proxy_socket_, buffers,
            strand_.wrap(
            [handler](const boost::system::error_code &ec, size_t) {
                (*handler)(ec);
            }));
}

} // namespace proxy
} // namespace one
