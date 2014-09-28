/*********************************************************************
*  @author Rafal Slota
*  @copyright (C): 2014 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/


#include "tls_server.h"
#include "log_message.h"

#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <fstream>

namespace one {
namespace proxy {

tls_server::tls_server(boost::asio::io_service &client_io_service,
                       boost::asio::io_service &proxy_io_service,
                       int verify_type, std::string cert_path,
                       uint16_t server_port, std::string forward_host,
                       uint16_t forward_port,
                       const std::vector<std::string> &ca_dirs)
    : client_io_service_(client_io_service)
    , proxy_io_service_(proxy_io_service)
    , verify_type_(verify_type)
    , acceptor_(client_io_service, boost::asio::ip::tcp::endpoint(
                                       boost::asio::ip::tcp::v4(), server_port))
    , context_(boost::asio::ssl::context::sslv23_server)
    , listen_port_(server_port)
    , forward_host_(forward_host)
    , ca_dirs_(ca_dirs)

{
    forward_port_ = std::to_string(forward_port);
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    context_.set_options(boost::asio::ssl::context::default_workarounds
                         | boost::asio::ssl::context::no_sslv2
                         | boost::asio::ssl::context::single_dh_use);

    context_.use_certificate_chain_file(cert_path);
    context_.use_private_key_file(cert_path, boost::asio::ssl::context::pem);
    context_.set_verify_mode(verify_type_);

    auto ciphers = "HIGH:!aNULL:!MD5";

    if (!SSL_CTX_set_cipher_list(context_.native_handle(), ciphers)) {
        LOG(WARNING) << "Cannot set cipther list (" << ciphers
                     << "). Server will accept weak ciphers!";
    }

    load_certs();
}

void tls_server::start_accept()
{
    auto new_session = boost::make_shared<tls2tcp_session>(
        shared_from_this(), client_io_service_, proxy_io_service_, context_,
        forward_host_, forward_port_);

    acceptor_.async_accept(new_session->socket(),
                           boost::bind(&tls_server::handle_accept,
                                       shared_from_this(), new_session,
                                       boost::asio::placeholders::error));
}

void tls_server::handle_accept(boost::shared_ptr<tls2tcp_session> new_session,
                               const boost::system::error_code &error)
{
    if (!error) {
        LOG(DEBUG) << "Starting new session...";
        new_session->start();
    }

    start_accept();
}

void tls_server::load_certs()
{
    std::vector<std::string> crls;
    std::vector<std::string> ca_certs;

    try
    {
        using namespace boost::filesystem;
        for (auto dir_name : ca_dirs_) {
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
    catch (boost::system::error_code &e)
    {
        LOG(ERROR) << "Cannot load CAs due to exception: " << e.message();
    }
    catch (std::exception &e)
    {
        LOG(ERROR) << "Cannot load CAs due to exception: " << e.what();
    }

    std::lock_guard<std::mutex> guard(certs_mutex_);
    ca_certs_ = ca_certs;
    crls_ = crls;

    LOG(INFO) << "Loaded " << ca_certs_.size() << " CAs and " << crls_.size()
              << " CRLs";
}

const std::vector<std::string> &tls_server::get_crl() const
{
    std::lock_guard<std::mutex> guard(certs_mutex_);
    return crls_;
}

const std::vector<std::string> &tls_server::get_ca() const
{
    std::lock_guard<std::mutex> guard(certs_mutex_);
    return ca_certs_;
}

void tls_server::register_session(const std::string &session_id,
                                  const std::string &session_data)
{
    std::lock_guard<std::mutex> guard(session_mutex_);
    sessions_[session_id] = session_data;
}

void tls_server::remove_session(const std::string &session_id)
{
    std::lock_guard<std::mutex> guard(session_mutex_);
    sessions_.erase(session_id);
}

std::string tls_server::get_session(const std::string &session_id) const
{
    std::lock_guard<std::mutex> guard(session_mutex_);
    auto itr = sessions_.find(session_id);
    if (itr != sessions_.end()) {
        return itr->second;
    } else {
        return "";
    }
}

} // namespace proxy
} // namespace one
