#ifndef tls_server_h
#define tls_server_h

#include "tls2tcp_session.h"

#include <cstdint>
#include <map>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/enable_shared_from_this.hpp>

namespace one {
namespace proxy {

    class tls_server : public boost::enable_shared_from_this<tls_server> {
    public:
        tls_server(boost::asio::io_service &client_io_service,
                   boost::asio::io_service &proxy_io_service, int verify_type,
                   std::string cert_path, uint16_t server_port,
                   std::string forward_host, uint16_t forward_port,
                   const std::vector<std::string> &);

        void start_accept();

        void handle_accept(boost::shared_ptr<tls2tcp_session> new_session,
                           const boost::system::error_code &error);

        const std::vector<std::string> &get_crl();
        const std::vector<std::string> &get_ca();

        void register_session(const std::string &session_id, const std::string &session_data);
        std::string get_session(const std::string &session_id);
        void remove_session(const std::string &session_id);

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
    };

} // namespace proxy
} // namespace one

#endif // tls_server_h