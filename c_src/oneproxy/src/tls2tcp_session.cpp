#include "tls2tcp_session.h"

#include "log_message.h"
#include "tls_server.h"
#include "base64.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "gpv/grid_proxy_verify.h"

#ifdef __cplusplus
}
#endif

#include <boost/bind.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <string>
#include <system_error>
#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/rand.h>

using std::string;

namespace one {
namespace proxy {

    tls2tcp_session::tls2tcp_session(boost::weak_ptr<tls_server> server,
                                     boost::asio::io_service &client_io_service,
                                     boost::asio::io_service &proxy_io_service,
                                     boost::asio::ssl::context &context,
                                     const std::string &forward_host,
                                     const std::string &forward_port)
        : server_(server)
        , client_socket_(client_io_service, context)
        , proxy_socket_(proxy_io_service)
        , proxy_io_service_(proxy_io_service)
        , forward_host_(forward_host)
        , forward_port_(forward_port)
        , client_buffer_(
              boost::asio::mutable_buffers_1(proxy_data_, buffer_size))
        , proxy_buffer_(
              boost::asio::mutable_buffers_1(proxy_data_, buffer_size))
        , peer_cert_(0)
    {
        unsigned char session_id_bin[SESSION_ID_SIZE];
        if(RAND_bytes(session_id_bin, SESSION_ID_SIZE)) {
            session_id_ = utils::base64_encode(std::string((const char *) session_id_bin, SESSION_ID_SIZE));
        } else {
            //throw std::system_error("Could not generate session id");
        }
    }

    tls2tcp_session::~tls2tcp_session()
    {
        if (peer_cert_)
            X509_free(peer_cert_);

        auto server = server_.lock();
        if(server) {
            server->remove_session(session_id_);
        }
    }

    ssl_socket::lowest_layer_type &tls2tcp_session::socket()
    {
        return client_socket_.lowest_layer();
    }

    ssl_socket &tls2tcp_session::sslsocket()
    {
        return client_socket_;
    }

    bool tls2tcp_session::handle_verify_certificate(
        bool preverified, boost::asio::ssl::verify_context &ctx)
    {
        char *x509_buf = 0;
        X509 *cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
        size_t der_size = i2d_X509(cert, (unsigned char **)&x509_buf);

        std::string der_cert(x509_buf, der_size);
        delete x509_buf;

        client_cert_chain_.push_back(der_cert);

        char subject_name[256];
        X509_NAME_oneline(X509_get_subject_name(cert), subject_name, 256);
        LOG(DEBUG) << "Verifying " << subject_name;

        return true;
    }

    std::string tls2tcp_session::gen_session_data()
    {
        auto der_2_pem = [](std::string der) -> std::string {
            std::string pem;

            X509 *peer_cert = 0;
            const unsigned char *p = (const unsigned char *)der.data();
            if (d2i_X509(&peer_cert, &p, der.size())) {
                BIO* mem = BIO_new(BIO_s_mem());
                auto buff = BUF_MEM_new();
                BIO_set_mem_buf(mem, buff, 0);
                if(PEM_write_bio_X509(mem, peer_cert)) {
                    pem = std::string(buff->data, buff->length);
                }

                BIO_free(mem);
            }

            return std::move(pem);
        };

        std::string pem_chain;
        for(auto &der : client_cert_chain_) {
            pem_chain += der_2_pem(der);
        }

        return utils::base64_encode(pem_chain);

    }

    void tls2tcp_session::start()
    {
        client_cert_chain_.clear();
        client_socket_.async_handshake(
            boost::asio::ssl::stream_base::server,
            boost::bind(&tls2tcp_session::handle_handshake, shared_from_this(),
                        boost::asio::placeholders::error));
    }

    bool tls2tcp_session::do_verify_peer()
    {
        if (client_cert_chain_.size() > 0) {
            auto server = server_.lock();
            if (!server)
                return false;

            auto &crls = server->get_crl();
            auto &cas = server->get_ca();

            GPV_CTX ctx;
            gpv_status result;

            // Init gpv_ctx
            result = gpv_init(&ctx);
            if (result != GPV_SUCCESS) {
                LOG(ERROR) << "Cannot initialize GPV CTX";
                return false;
            }

            result = gpv_set_leaf_cert(&ctx, client_cert_chain_.back().data(),
                                       client_cert_chain_.back().size());
            if (result != GPV_SUCCESS) {
                LOG(ERROR) << "Cannot setup peer cert for GPV CTX";
                return false;
            }

            for (auto &cert : client_cert_chain_) {
                gpv_add_chain_cert(&ctx, cert.data(), cert.size());
            }

            for (auto &crl : crls) {
                gpv_add_crl_cert(&ctx, crl.data(), crl.size());
            }

            for (auto &ca : cas) {
                gpv_add_trusted_ca(&ctx, ca.data(), ca.size());
            }

            result = gpv_verify(&ctx);
            auto error = gpv_get_error(&ctx);

            gpv_cleanup(&ctx);

            X509 *peer_cert = 0;
            const unsigned char *p
                = (const unsigned char *)client_cert_chain_.back().data();
            // memcmp(p, , client_cert_chain_.back().size());
            if (!d2i_X509(&peer_cert, &p, client_cert_chain_.back().size())) {
                LOG(ERROR) << "Could not convert peer cert to internal format";
                return false;
            }

            if (result == GPV_SUCCESS) {
                peer_cert_ = peer_cert;
                return true;
            } else {
                char subject_name[256];
                X509_NAME_oneline(X509_get_subject_name(peer_cert),
                                  subject_name, 256);
                LOG(INFO) << "Peer " << subject_name
                          << " rejected due to SSL error code: {" << result << ", " << error << "}";
            }
        }

        return false;
    }

    void tls2tcp_session::handle_handshake(const boost::system::error_code
                                           &error)
    {
        if (!error) {
            try
            {
                auto verified = do_verify_peer();
                if(verified) {
                    auto data = gen_session_data();
                    auto server = server_.lock();
                    if(server)
                        server->register_session(session_id_, data);
                }

                using boost::asio::ip::tcp;

                tcp::resolver resolver(proxy_io_service_);
                tcp::resolver::query query(forward_host_, forward_port_);

                tcp::resolver::iterator endpoint_iterator
                    = resolver.resolve(query);
                tcp::resolver::iterator end;

                boost::system::error_code error = boost::asio::error::host_not_found;
                while (error && endpoint_iterator != end) {
                    proxy_socket_.close();
                    proxy_socket_.connect(*endpoint_iterator++, error);
                }

                if (error) {
                    LOG(ERROR) << "Proxy endpoint (" << forward_host_ << ":"
                               << forward_port_
                               << ") unreachable: " << error.message();
                    return;
                }

                // Setup sockets
                proxy_socket_.set_option(boost::asio::ip::tcp::no_delay(true));
                socket().set_option(boost::asio::ip::tcp::no_delay(true));

                boost::asio::streambuf request(buffer_size);
                boost::asio::streambuf forward_request;
                std::istream requestStream(&request);
                std::ostream requestForwardStream(&forward_request);

                auto proxy_flush = [&]() {
                    requestForwardStream.flush();
                    auto pending = forward_request.size();
                    auto written
                        = boost::asio::write(proxy_socket_, forward_request);
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

                LOG(DEBUG) << "Got HTTP init request: " << method << " " << uri
                           << " " << http;

                requestForwardStream << method << " " << uri << " " << http
                                     << "\r\n";
                proxy_flush();

                while ((header_size = boost::asio::read_until(
                            client_socket_, request, "\r\n")) > 2) {
                    requestStream.getline(
                        client_data_,
                        std::min((size_t)buffer_size, header_size), ':');
                    key = string(client_data_);
                    requestStream.getline(
                        client_data_,
                        std::min((size_t)buffer_size, header_size), '\r');
                    value = string(client_data_);
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
                    char subject_name[2048];
                    X509_NAME_oneline(X509_get_subject_name(peer_cert_), subject_name, 2048);
                    custom_headers.push_back(std::tuple<string, string>{
                        std::string(INTERNAL_HEADER_PREFIX) + "client-subject-dn", subject_name});

                    custom_headers.push_back(std::tuple<string, string>{
                        std::string(INTERNAL_HEADER_PREFIX) + "client-session-id", session_id_});
                }

                for (auto &header : custom_headers) {
                    std::tie(key, value) = header;
                    LOG(DEBUG) << "Adding custom header: " << key << " => "
                               << value;
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

                // Start reading...
                proxy_socket_.async_read_some(
                    boost::asio::buffer(proxy_data_, buffer_size),
                    boost::bind(&tls2tcp_session::handle_proxy_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));

                client_socket_.async_read_some(
                    boost::asio::buffer(client_data_, buffer_size),
                    boost::bind(&tls2tcp_session::handle_client_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
            }
            catch (boost::system::error_code &e)
            {
                LOG(ERROR) << "Cannot initialize proxy connection due to: "
                           << e.message();
            }
            catch (boost::exception &e)
            {
                LOG(DEBUG) << "Unknown handshake error";
            }
        } else {
            handle_error("handle_handshake", error);
        }
    }

    void tls2tcp_session::handle_client_read(const boost::system::error_code
                                             &error,
                                             size_t bytes_transferred)
    {
        if (!error) {
            boost::asio::async_write(
                proxy_socket_,
                boost::asio::const_buffers_1(client_data_, bytes_transferred),
                boost::bind(&tls2tcp_session::handle_proxy_write,
                            shared_from_this(),
                            boost::asio::placeholders::error));
        } else {
            handle_error("handle_client_read", error);
        }
    }

    void tls2tcp_session::handle_proxy_read(const boost::system::error_code
                                            &error,
                                            size_t bytes_transferred)
    {
        if (!error) {
            boost::asio::async_write(
                client_socket_,
                boost::asio::const_buffers_1(proxy_data_, bytes_transferred),
                boost::bind(&tls2tcp_session::handle_client_write,
                            shared_from_this(),
                            boost::asio::placeholders::error));
        } else {
            handle_error("handle_proxy_read", error);
        }
    }

    void tls2tcp_session::handle_client_write(const boost::system::error_code
                                              &error)
    {
        if (!error) {
            proxy_socket_.async_read_some(
                boost::asio::mutable_buffers_1(proxy_data_, buffer_size),
                boost::bind(&tls2tcp_session::handle_proxy_read,
                            shared_from_this(),
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
        } else {
            handle_error("handle_client_write", error);
        }
    }

    void tls2tcp_session::handle_proxy_write(const boost::system::error_code
                                             &error)
    {
        if (!error) {
            client_socket_.async_read_some(
                boost::asio::mutable_buffers_1(client_data_, buffer_size),
                boost::bind(&tls2tcp_session::handle_client_read,
                            shared_from_this(),
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
        } else {
            handle_error("handle_proxy_write", error);
        }
    }

    void tls2tcp_session::handle_error(const std::string &method,
                                       const boost::system::error_code &error)
    {
        using namespace boost::asio::error;

        if (error == eof) {
            LOG(DEBUG) << "Connection closed...";
        } else if (error == operation_aborted) {
            LOG(DEBUG) << "Connection aborted...";
        } else {
            LOG(ERROR) << method << " failed due to: " << error.message();
        }

        try
        {
            if (proxy_socket_.is_open())
                proxy_socket_.close();
            if (socket().is_open())
                socket().close();
        }
        catch (boost::system::error_code &e)
        {
            if (e == bad_descriptor) {
                LOG(DEBUG)
                    << "Cannot close socket because it is already closed";
            } else {
                LOG(WARNING) << "Cannot close socket due to: " << e.message();
            }
        }
        catch (std::exception &e)
        {
            LOG(DEBUG) << "Unknown session error " << e.what();
        }
    }
}
}