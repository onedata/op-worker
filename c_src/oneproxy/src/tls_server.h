#ifndef tls_server_h
#define tls_server_h

#include "tls2tcp_session.h"

#include <cstdint>
#include <map>
#include <mutex>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/enable_shared_from_this.hpp>

namespace one {
namespace proxy {

/**
 * The tls_server class.
 * Represents single proxy server instance.
 */
class tls_server : public boost::enable_shared_from_this<tls_server> {
public:

    /**
     * Iitializes server's state.
     * @param client_io_service io_service used for client's I/O
     * @param proxy_io_service io_service used for providers's I/O
     * @param verify_type OpenSSL/Boost verify type flag (like @c boost::asio::ssl::verify_peer)
     * @param cert_path Path to proxy's certificate chain used on server endpoint
     * @param server_port Port of the proxy server
     * @param forward_host Host to which data shall be redirected
     * @param forward_port Port to which data shall be redirected
     * @param ca_crl_paths Paths to directories with CAs and CRLs. All certs have to be in DER format and have .der and .crl extension respectively.
     */
    tls_server(boost::asio::io_service &client_io_service,
               boost::asio::io_service &proxy_io_service, int verify_type,
               std::string cert_path, uint16_t server_port,
               std::string forward_host, uint16_t forward_port,
               const std::vector<std::string> &ca_crl_paths);

    /**
     * Starts proxy server.
     */
    void start_accept();



    /// Getter for CRL DERs
    const std::vector<std::string> &get_crl() const;

    /// Getter for CA DERs
    const std::vector<std::string> &get_ca() const;

    /**
     * Register session's data for given session id.
     * @param session_id Session ID
     * @param session_data Session's arbitrary data.
     */
    void register_session(const std::string &session_id,
                          const std::string &session_data);

    /**
     * Returns session data.
     * @param session_id Session ID
     * @return Session's data. If session does not exist, empty data will be retured.
     */
    std::string get_session(const std::string &session_id) const;

    /**
     * Remove session data for given session
     * @param session_id Session ID
     */
    void remove_session(const std::string &session_id);

    /**
     * (Re)Loads CAs and CRLs from hard drive.
     */
    void load_certs();

private:
    boost::asio::io_service &client_io_service_;
    boost::asio::io_service &proxy_io_service_;
    const int verify_type_;
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::ssl::context context_;

    const uint16_t listen_port_;
    const std::string forward_host_;
    std::string forward_port_;

    const std::vector<std::string> ca_dirs_;

    std::vector<std::string> crls_;
    std::vector<std::string> ca_certs_;

    std::map<std::string, std::string> sessions_;

    mutable std::mutex certs_mutex_;
    mutable std::mutex session_mutex_;

    void handle_accept(boost::shared_ptr<tls2tcp_session> new_session,
                       const boost::system::error_code &error);
};

} // namespace proxy
} // namespace one

#endif // tls_server_h
