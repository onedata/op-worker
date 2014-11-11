/**
 * @author Rafal Slota
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#include "server.h"

#include "log_message.h"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <fstream>

namespace one {
namespace proxy {

server::server(boost::asio::io_service &client_io_service,
               boost::asio::io_service &proxy_io_service, int verify_type,
               uint16_t server_port, std::vector<std::string> ca_crl_paths)
    : client_io_service_(client_io_service)
    , proxy_io_service_(proxy_io_service)
    , verify_type_(verify_type)
    , listen_port_(server_port)
    , ca_crl_dirs_(std::move(ca_crl_paths))
{
}

void server::load_certs()
{
    std::vector<std::string> crls;
    std::vector<std::string> ca_certs;

    try {
        using namespace boost::filesystem;
        for (auto dir_name : ca_crl_dirs_) {
            if (!exists(dir_name) || !is_directory(dir_name)) {
                LOG(WARNING) << "CA directory " << dir_name
                             << " does not exist!";
                continue;
            }
            directory_iterator end_itr; // default construction yields
                                        // past-the-end
            for (directory_iterator itr(dir_name); itr != end_itr; ++itr) {
                if (is_directory(itr->status()))
                    continue; // Skip sub-directories

                auto file_name = itr->path().string();

                std::ifstream input(file_name, std::ios::binary);

                if (boost::algorithm::ends_with(file_name, ".crl")) {
                    crls.push_back(
                        std::string(std::istreambuf_iterator<char>(input),
                                    std::istreambuf_iterator<char>()));
                } else if (boost::algorithm::ends_with(file_name, ".crt")) {
                    ca_certs.push_back(
                        std::string(std::istreambuf_iterator<char>(input),
                                    std::istreambuf_iterator<char>()));
                } else {
                    LOG(WARNING) << "Unknown certificate file " << file_name
                                 << ". Skipping.";
                }
            }
        }
    }
    catch (boost::system::error_code &e) {
        LOG(ERROR) << "Cannot load CAs due to exception: " << e.message();
    }
    catch (std::exception &e) {
        LOG(ERROR) << "Cannot load CAs due to exception: " << e.what();
    }

    std::lock_guard<std::mutex> guard(certs_mutex_);
    ca_certs_ = ca_certs;
    crls_ = crls;

    LOG(INFO) << "Loaded " << ca_certs_.size() << " CAs and " << crls_.size()
              << " CRLs";
}

void server::init_server(boost::asio::ip::tcp::acceptor &acceptor,
                         boost::asio::ssl::context &context,
                         const std::string &cert_path)
{
    acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    context.set_options(boost::asio::ssl::context::default_workarounds |
                        boost::asio::ssl::context::no_sslv2 |
                        boost::asio::ssl::context::single_dh_use);

    context.use_certificate_chain_file(cert_path);
    context.use_private_key_file(cert_path, boost::asio::ssl::context::pem);
    context.set_verify_mode(verify_type_);

    auto ciphers = "HIGH:!aNULL:!MD5";

    if (!SSL_CTX_set_cipher_list(context.native_handle(), ciphers)) {
        LOG(WARNING) << "Cannot set cipther list (" << ciphers
                     << "). Server will accept weak ciphers!";
    }

    load_certs();
}

const std::vector<std::string> &server::get_crl() const
{
    std::lock_guard<std::mutex> guard(certs_mutex_);
    return crls_;
}

const std::vector<std::string> &server::get_ca() const
{
    std::lock_guard<std::mutex> guard(certs_mutex_);
    return ca_certs_;
}

void server::register_session(const std::string &session_id,
                              std::string session_data)
{
    std::lock_guard<std::mutex> guard(session_mutex_);
    sessions_[session_id] = std::move(session_data);
}

void server::remove_session(const std::string &session_id)
{
    std::lock_guard<std::mutex> guard(session_mutex_);
    sessions_.erase(session_id);
}

std::string server::get_session(const std::string &session_id) const
{
    std::lock_guard<std::mutex> guard(session_mutex_);
    auto itr = sessions_.find(session_id);
    if (itr != sessions_.end()) {
        return itr->second;
    }

    return {};
}

} // namespace proxy
} // namespace one
