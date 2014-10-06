/*********************************************************************
 * @author Rafal Slota
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/


#include "log_message.h"
#include "tls_server.h"

#include <iostream>
#include <vector>
#include <cstdio>
#include <thread>

#include <boost/make_shared.hpp>
#include <thread>

namespace one {
namespace proxy {
std::mutex stdout_mutex;
}
}

/// Default threads count. Used only when automatic core-count detection failes.
constexpr uint16_t WORKER_COUNT = 8;

using std::atoi;
using namespace one::proxy;


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

int main(int argc, char *argv[])
{
    std::ios_base::sync_with_stdio(false);
    try
    {
        if (argc < 6) {
            LOG(ERROR) << "Invalid argument. Usage: " << argv[0]
                       << " <listen_port> <forward_host> <forward_port> "
                       << "cert_path verify_peer|verify_none [ca|crl_dir...]";
            return EXIT_FAILURE;
        }

        boost::asio::io_service client_io_service;
        boost::asio::io_service proxy_io_service;
        std::vector<std::thread> workers;

        {
            auto verify_type = (std::string(argv[5]) == "verify_peer"
                                    ? boost::asio::ssl::verify_peer
                                    : boost::asio::ssl::verify_none);
            std::vector<std::string> ca_dirs{argv + 6, argv + argc};

            auto s = std::make_shared<tls_server>(
                client_io_service, proxy_io_service, verify_type, argv[4],
                atoi(argv[1]), argv[2], atoi(argv[3]), ca_dirs);

            s->start_accept();

            boost::asio::io_service::work client_work(client_io_service);
            boost::asio::io_service::work proxy_work(proxy_io_service);

            auto worker_count = std::thread::hardware_concurrency() / 2;
            if (!worker_count)
                worker_count = WORKER_COUNT / 2;

            if (worker_count <= 0) {
                LOG(ERROR) << "Incorrect number of workers";
                return EXIT_FAILURE;
            }

            for (auto i = 0u; i < WORKER_COUNT; ++i) {
                workers.push_back(
                    std::thread{[&]() { client_io_service.run(); }});
                workers.push_back(
                    std::thread{[&]() { proxy_io_service.run(); }});
            }

            LOG(INFO) << "Proxy 0.0.0.0:" << atoi(argv[1]) << " -> " << argv[2]
                      << ":" << atoi(argv[3]) << " has started with "
                      << (worker_count * 2) << " workers";

            // Simple erlang port dirver
            std::string command, message_id, arg0;
            for(std::string line; std::getline(std::cin, line); ) {
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

        client_io_service.stop();
        proxy_io_service.stop();

        for (auto &worker : workers) {
            worker.join();
        }
    }
    catch (std::exception &e)
    {
        LOG(ERROR) << "Proxy failed due to exception: " << e.what();
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
