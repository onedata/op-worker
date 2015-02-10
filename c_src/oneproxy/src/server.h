/**
 * @author Rafal Slota
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license cited in 'LICENSE.txt'.
 */

#ifndef SERVER_H
#define SERVER_H

#include "log_message.h"

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

namespace one {
namespace proxy {

/**
 * The server class is a base class for classes representing a single proxy
 * server instance.
 */
class server {
public:
    /**
     * Iitializes server's state.
     * @param client_io_service io_service used for client's I/O
     * @param proxy_io_service io_service used for providers's I/O
     * @param verify_type OpenSSL/Boost verify type flag
     * (like @c boost::asio::ssl::verify_peer)
     * @param server_port Port of the proxy server
     * @param ca_crl_paths Paths to directories with CAs and CRLs. All certs
     * have to be in DER format and have .der and .crl extension respectively.
     */
    server(boost::asio::io_service &client_io_service,
           boost::asio::io_service &proxy_io_service, int verify_type,
           uint16_t server_port, std::vector<std::string> ca_crl_paths);

    virtual ~server() = default;

    /**
     * Starts proxy server.
     */
    virtual void start_accept() = 0;

    /**
     * Getter for CRL DERs
     */
    const std::vector<std::string> &get_crl() const;

    /**
     * Getter for CA DERs
     */
    const std::vector<std::string> &get_ca() const;

    /**
     * Register session's data for given session id.
     * @param session_id Session ID
     * @param session_data Session's arbitrary data.
     */
    void register_session(const std::string &session_id,
                          std::string session_data);

    /**
     * Returns session data.
     * @param session_id Session ID
     * @return Session's data. If session does not exist, empty data will be
     * retured.
     */
    std::string get_session(const std::string &session_id) const;

    /**
     * Remove session data for given session
     * @param session_id Session ID
     */
    void remove_session(const std::string &session_id);

    /**
     * (Re)loads CAs and CRLs from hard drive.
     */
    void load_certs();

protected:
    template <class session_like>
    void handle_accept(std::shared_ptr<session_like> new_session,
                       const boost::system::error_code &error)
    {
        if (!error) {
            LOG(DEBUG) << "Starting new session...";
            new_session->start();
        }

        start_accept();
    }

    void init_server(boost::asio::ip::tcp::acceptor &acceptor,
                     boost::asio::ssl::context &context,
                     const std::string &cert_path);

    boost::asio::io_service &client_io_service_;
    boost::asio::io_service &proxy_io_service_;
    const int verify_type_;

private:
    const std::vector<std::string> ca_crl_dirs_;

    std::vector<std::string> crls_;
    std::vector<std::string> ca_certs_;

    std::map<std::string, std::string> sessions_;

    mutable std::mutex certs_mutex_;
    mutable std::mutex session_mutex_;
};

} // namespace proxy
} // namespace one

#endif // SERVER_H
