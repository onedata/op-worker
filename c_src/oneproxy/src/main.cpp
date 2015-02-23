/**
 * @file main.cpp
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "log_message.h"
#include "tls_server.h"
#include "tls2tcp_session.h"
#include "tls2tcp_http_session.h"
#include "tcp_server.h"

#include <cstdlib>
#include <iostream>
#include <vector>
#include <string>
#include <thread>

#include <boost/make_shared.hpp>

namespace one {
namespace proxy {
std::mutex stdout_mutex;
}
}

/// Default threads count. Used only when automatic core-count detection failes.
constexpr uint16_t WORKER_COUNT = 8;

using std::atoi;
using namespace one::proxy;
using namespace std::literals::string_literals;

/**
 * Write erlang-port response.
 * @param tokens String tokens that shall be returned to erlang port driver
 */
void command_response(std::vector<std::string> tokens)
{
    if (tokens.size() == 0)
        return;

    std::lock_guard<std::mutex> guard(stdout_mutex);

    std::cout << tokens[0];
    for (auto i = 1u; i < tokens.size(); ++i) {
        std::cout << " " << tokens[i];
    }

    std::cout << std::endl;
}

/**
 * The mode in which the proxy works.
 * @c normal is a standard proxy mode, @c reverse is a reverse proxy mode.
 */
enum class proxy_type { reverse, normal };

/**
 * Prints a usage message.
 * @param app_name Name of the application as present in @c argv[0]
 */
void invalid_argument(const std::string &app_name)
{
    LOG(ERROR) << "Invalid argument. Usage:\n\t" << app_name
               << " reverse_proxy <listen_port> <forward_host> <forward_port> "
                  "<cert_path> verify_peer|verify_none http|no_http "
                  "[ca|crl_dir...]\n\t" << app_name
               << " proxy <listen_port> <cert_path> "
                  "verify_peer|verify_none [ca|crl_dir...]";
}

int main(int argc, char *argv[])
{
    std::ios_base::sync_with_stdio(false);
    try {
        if (argc < 4) {
            invalid_argument(argv[0]);
            return EXIT_FAILURE;
        }

        boost::asio::io_service io_service;
        boost::asio::io_service::strand strand(io_service);
        std::vector<std::thread> workers;

        {
            const auto type =
                argv[1] == "proxy"s ? proxy_type::normal : proxy_type::reverse;

            if (type == proxy_type::reverse && argc < 6) {
                invalid_argument(argv[0]);
                return EXIT_FAILURE;
            }

            const auto verify_type_ind = type == proxy_type::reverse ? 6 : 4;
            const auto ca_dirs_ind = type == proxy_type::reverse ? 8 : 5;

            auto verify_type = (argv[verify_type_ind] == "verify_peer"s
                                    ? boost::asio::ssl::verify_peer
                                    : boost::asio::ssl::verify_none);

            std::vector<std::string> ca_dirs{argv + ca_dirs_ind, argv + argc};

            std::shared_ptr<server> s;
            switch (type) {
                case proxy_type::reverse: {
                    const auto listen_port = atoi(argv[2]);
                    const auto forward_host = argv[3];
                    const auto forward_port = atoi(argv[4]);
                    const auto cert_path = argv[5];
                    const auto http_mode = argv[7];

                    if (http_mode == "http"s) {
                        s = std::make_shared<tls_server<tls2tcp_http_session>>(
                                io_service, strand, verify_type,
                            cert_path, listen_port, forward_host, forward_port,
                            ca_dirs);
                    } else {
                        s = std::make_shared<tls_server<tls2tcp_session>>(
                                io_service, strand, verify_type,
                            cert_path, listen_port, forward_host, forward_port,
                            ca_dirs);
                    }
                    break;
                }

                case proxy_type::normal: {
                    const auto cert_path = argv[3];
                    const auto listen_port = atoi(argv[2]);

                    s = std::make_shared<tcp_server>(
                            io_service, strand, verify_type,
                        cert_path, listen_port, ca_dirs);
                    break;
                }
            }

            s->start_accept();

            boost::asio::io_service::work work(io_service);

            auto worker_count = std::thread::hardware_concurrency();
            if (!worker_count)
                worker_count = WORKER_COUNT;

            if (worker_count <= 0) {
                LOG(ERROR) << "Incorrect number of workers";
                return EXIT_FAILURE;
            }

            for (auto i = 0u; i < WORKER_COUNT; ++i) {
                workers.push_back(
                    std::thread{[&]() { io_service.run(); }});
            }

            if (type == proxy_type::reverse) {
                LOG(INFO) << "Reverse-Proxy 0.0.0.0:" << atoi(argv[2]) << " -> "
                          << argv[3] << ":" << atoi(argv[4])
                          << " has started with " << (worker_count * 2)
                          << " workers";
            } else {
                LOG(INFO) << "Proxy 127.0.0.1:" << atoi(argv[2])
                          << " -> the Internet has started with "
                          << (worker_count * 2) << " workers";
            }

            // Simple erlang port dirver
            std::string command, message_id, arg0;
            for (std::string line; std::getline(std::cin, line);) {
                std::stringstream line_stream(line);

                line_stream >> command;
                if (command == "reload_certs") {
                    s->load_certs();
                } else if (command == "q") {
                    break;
                } else if (command == "get_session") {
                    line_stream >> message_id >> arg0;
                    command_response({message_id, s->get_session(arg0)});
                } else if (command == "heartbeat") {
                    // Do basically nothing
                } else {
                    LOG(ERROR) << "Unknown command '" << command << "'";
                }
            }
        }

        LOG(INFO) << "Stopping proxy on port " << atoi(argv[1]) << "...";

        io_service.stop();

        for (auto &worker : workers) {
            worker.join();
        }
    }
    catch (std::exception &e) {
        LOG(ERROR) << "Proxy failed due to exception: " << e.what();
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
