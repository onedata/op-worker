#include "log_message.h"
#include "tls_server.h"

#include <iostream>
#include <vector>

#include <boost/thread/scoped_thread.hpp>
#include <boost/thread.hpp>
#include <boost/make_shared.hpp>
#include <thread>

constexpr uint16_t WORKER_COUNT = 8;

typedef boost::scoped_thread<boost::interrupt_and_join_if_joinable> auto_thread;
using std::atoi;

using namespace one::proxy;

int main(int argc, char *argv[])
{
    try
    {
        if (argc < 6) {
            LOG(INFO) << "Invalid argument. Usage: " << argv[0]
                      << " <listen_port> <forward_host> <forward_port> "
                      << "cert_path verify_peer|verify_none [ca|crl_dir...]";
            return EXIT_FAILURE;
        }

        boost::asio::io_service client_io_service;
        boost::asio::io_service proxy_io_service;
        std::vector<auto_thread> workers;

        {
            auto verify_type = (std::string(argv[5]) == "verify_peer"
                                    ? boost::asio::ssl::verify_peer
                                    : boost::asio::ssl::verify_none);
            std::vector<std::string> ca_dirs;
            for (int i = 6; i < argc; ++i) {
                ca_dirs.push_back(argv[i]);
            }

            auto s = boost::make_shared<tls_server>(
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

            for (uint16_t i = 0; i < WORKER_COUNT; ++i) {
                workers.push_back(
                    auto_thread{[&]() { client_io_service.run(); }});
                workers.push_back(
                    auto_thread{[&]() { proxy_io_service.run(); }});
            }

            LOG(INFO) << "Proxy 0.0.0.0:" << atoi(argv[1]) << " -> " << argv[2]
                      << ":" << atoi(argv[3]) << " has started with "
                      << (worker_count * 2) << " workers";

            std::string command, arg0;
            while (std::cin.good()) {
                std::cin >> command;
                if (command == "reload_certs") {
                    s->load_certs();
                } else if (command == "q") {
                    break;
                } else if (command == "get_session") {
                    std::cin >> arg0;
                    std::cout << s->get_session(arg0) << std::endl;
                    std::cout.flush();
                } else {
                    LOG(ERROR) << "Unknown command '" << command << "'";
                }
            }
        }

        LOG(INFO) << "Stopping proxy on port " << atoi(argv[1]) << "...";

        client_io_service.stop();
        proxy_io_service.stop();
    }
    catch (std::exception &e)
    {
        LOG(ERROR) << "Proxy failed due to exception: " << e.what();
    }

    return EXIT_SUCCESS;
}