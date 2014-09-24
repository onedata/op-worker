#ifndef tls2tcp_session_h
#define tls2tcp_session_h

#include "tls2tcp_session.h"

#include <vector>
#include <string>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio/buffer.hpp>

namespace one {
namespace proxy {

    constexpr const char *INTERNAL_HEADER_PREFIX = "onedata-internal-";
    constexpr const size_t SESSION_ID_SIZE = 16; // in bytes

    class tls_server;

    typedef boost::asio::ip::tcp::socket tcp_socket;
    typedef boost::asio::ssl::stream<tcp_socket> ssl_socket;

    class tls2tcp_session : public boost::enable_shared_from_this
                            <tls2tcp_session> {
    public:
        tls2tcp_session(boost::weak_ptr<tls_server> server,
                        boost::asio::io_service &client_io_service,
                        boost::asio::io_service &proxy_io_service,
                        boost::asio::ssl::context &context,
                        const std::string &forward_host,
                        const std::string &forward_port);

        ~tls2tcp_session();

        ssl_socket::lowest_layer_type &socket();
        ssl_socket &sslsocket();

        bool do_verify_peer();

        void start();
        void handle_handshake(const boost::system::error_code &error);
        void handle_client_read(const boost::system::error_code &error,
                                size_t bytes_transferred);
        void handle_proxy_read(const boost::system::error_code &error,
                               size_t bytes_transferred);
        void handle_client_write(const boost::system::error_code &error);
        void handle_proxy_write(const boost::system::error_code &error);
        void handle_error(const std::string &method,
                          const boost::system::error_code &error);

        bool handle_verify_certificate(bool preverified,
                                       boost::asio::ssl::verify_context &ctx);

    private:
        enum {
            buffer_size = 1024 * 1024
        };

        boost::weak_ptr<tls_server> server_;

        ssl_socket client_socket_;
        tcp_socket proxy_socket_;
        boost::asio::io_service &proxy_io_service_;

        const std::string forward_host_;
        const std::string forward_port_;

        char client_data_[buffer_size];
        char proxy_data_[buffer_size];
        boost::asio::mutable_buffers_1 client_buffer_;
        boost::asio::mutable_buffers_1 proxy_buffer_;

        std::vector<std::string> client_cert_chain_;

        X509 *peer_cert_;

        std::string session_id_;


        std::string gen_session_data();
    };

} // namespace proxy
} // namespace one

#endif // tls2tcp_session_h