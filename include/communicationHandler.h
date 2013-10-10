/**
 * @file communicationHandler.h
 * @author Beata Skiba
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */


#ifndef COMMUNICATION_HANDLER_H
#define COMMUNICATION_HANDLER_H

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <string>
#include <boost/thread.hpp>
#include "communication_protocol.pb.h"
#include "veilErrors.h"

namespace veil {

/**
 * The CommunicationHandler class.
 * Object of this class represents dynamic (auto-connect) TCP/IP connection to cluster.
 * The CommunicationHandler allows to communicate with cluster by sending and receiving
 * ClusterMsg messages. TCP connection is fully automatic, which means that event if current CommunicationHandler
 * isnt connected, it will connect automatically. If it is, it will reuse previously opened socket.
 */
class CommunicationHandler
{

protected:
    int serverSocket;
    SSL_CTX * sslContext;
    SSL * ssl;

    std::string m_hostname;
    int m_port;
    std::string m_certPath;

    boost::mutex m_access;
    volatile static int instancesCount;

    virtual int openTCPConnection();                                ///< Opens INET socket.
    
    virtual int initCTX();                                          ///< Initialize certificates etc.
    virtual void initSSL();                                         ///< Initialize SSL context.
    virtual int writeBytes(uint8_t * msg_buffer, int size);         ///< Write given array of bytes to SSL socket. @return -1 on error. Otherwise number of bytes written.
    virtual int readBytes(uint8_t * msg_buffer, int size);          ///< Read bytes from SSL socket. @return -1 on error. Otherwise number of bytes read.

public:
    CommunicationHandler(std::string hostname, int port, std::string certPath);
    virtual ~CommunicationHandler();

    virtual int openConnection();                                   ///< Opens SSL connection.
    virtual void closeConnection();                                 ///< Closes active connection.
    virtual int sendMessage(const protocol::communication_protocol::ClusterMsg& message);             ///< Sends ClusterMsg using current SSL session. Will fail if there isnt one.
    virtual int receiveMessage(protocol::communication_protocol::Answer * answer);                    ///< Receives Answer using current SSL session. Will fail if there isnt one.
    virtual protocol::communication_protocol::Answer communicate(protocol::communication_protocol::ClusterMsg &msg, uint8_t retry);     ///< Sends ClusterMsg and receives answer. Same as running CommunicationHandler::sendMessage and CommunicationHandler::receiveMessage
                                                                    ///< but this method also supports reconnect option. If something goes wrong during communication, new connection will be
                                                                    ///< estabilished and the whole process will be repeated.
                                                                    ///< @param retry How many times tries has to be made before returning error.
                                                                    ///< @return Answer protobuf message. If error occures, empty Answer object will be returned.

    static int getInstancesCount();

};

} // namespace veil

#endif // COMMUNICATION_HANDLER_H
