/**
 * @file tls2tcp_http_session.cpp
 * @author Rafal Slota
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#include "tls2tcp_http_session.h"

#include "log_message.h"

#include <boost/algorithm/string.hpp>

#include <string>

namespace one {
namespace proxy {

void tls2tcp_http_session::start_reading(bool verified)
{
    boost::asio::streambuf request(buffer_size);
    boost::asio::streambuf forward_request;
    std::istream requestStream(&request);
    std::ostream requestForwardStream(&forward_request);

    auto proxy_flush = [&]() {
        requestForwardStream.flush();
        auto pending = forward_request.size();
        auto written = boost::asio::write(proxy_socket_, forward_request);
        if (pending != written) {
            LOG(WARNING) << "Short write while processing initial "
                            "HTTP request";
        }
    };

    std::string method;
    std::string uri;
    std::string http;
    std::string key, value;
    size_t header_size;

    if (!boost::asio::read_until(client_socket_, request, "\r\n")) {
        LOG(ERROR) << "Malformed HTTP request";
        return;
    }

    requestStream >> method >> uri >> http;
    request.consume(2);

    LOG(DEBUG) << "Got HTTP init request: " << method << " " << uri << " "
               << http;

    requestForwardStream << method << " " << uri << " " << http << "\r\n";
    proxy_flush();

    while ((header_size =
                boost::asio::read_until(client_socket_, request, "\r\n")) > 2) {
        requestStream.getline(
            client_data_.data(),
            std::min((size_t)client_data_.size(), header_size), ':');
        key = std::string(client_data_.data());
        requestStream.getline(
            client_data_.data(),
            std::min((size_t)client_data_.size(), header_size), '\r');
        value = std::string(client_data_.data());
        request.consume(1); // Consume '\n'

        LOG(DEBUG) << "Read HTTP header: " << key << " " << value;
        if (!boost::starts_with(key, INTERNAL_HEADER_PREFIX)) {
            requestForwardStream << key << ": " << value << "\r\n";
            requestForwardStream.flush();
        }

        if (forward_request.size() > buffer_size) {
            proxy_flush();
        }
    }

    std::vector<std::tuple<std::string, std::string>> custom_headers;
    if (verified) {
        std::array<char, 2048> subject_name;
        X509_NAME_oneline(X509_get_subject_name(peer_cert_),
                          subject_name.data(), subject_name.size());
        custom_headers.push_back(std::tuple<std::string, std::string>{
            std::string(INTERNAL_HEADER_PREFIX) + "client-subject-dn",
            subject_name.data()});

        custom_headers.push_back(std::tuple<std::string, std::string>{
            std::string(INTERNAL_HEADER_PREFIX) + "client-session-id",
            session_id_});
    }

    for (auto &header : custom_headers) {
        std::tie(key, value) = header;
        LOG(DEBUG) << "Adding custom header: " << key << " => " << value;
        requestForwardStream << key << ": " << value << "\r\n";
    }

    proxy_flush();
    auto pending = request.size();
    auto written = boost::asio::write(proxy_socket_, request);
    if (written != pending) {
        LOG(WARNING) << "Short write while flushing original "
                     << "initial HTTP request";
        return;
    }

    tls2tcp_session::start_reading(verified);
}

} // namespace proxy
} // namespace one
