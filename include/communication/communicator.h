/**
 * @file communicator.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATION_COMMUNICATOR_H
#define VEILHELPERS_COMMUNICATION_COMMUNICATOR_H


#include "communication/communicationHandler.h"
#include "make_unique.h"
#include "veilErrors.h"

#include <google/protobuf/message.h>

#include <chrono>
#include <memory>
#include <string>

namespace veil
{
namespace communication
{

static constexpr int PROTOCOL_VERSION = 1;

static constexpr const char
    *FSLOGIC_MODULE_NAME                = "fslogic",
    *CENTRAL_LOGGER_MODULE_NAME         = "central_logger",
    *REMOTE_FILE_MANAGEMENT_MODULE_NAME = "remote_files_manager",
    *RULE_MANAGER_MODULE_NAME           = "rule_manager",
    *CLUSTER_RENGINE_MODULE_NAME        = "cluster_rengine";

static constexpr std::chrono::seconds RECV_TIMEOUT{2};

class CertificateData;

class Communicator
{
    using Answer = protocol::communication_protocol::Answer;
    using ClusterMsg = protocol::communication_protocol::ClusterMsg;
    using Atom = veil::protocol::communication_protocol::Atom;

public:
    Communicator(std::unique_ptr<CommunicationHandler> communicationHandler,
                 std::string uri);

    virtual ~Communicator() = default;

    Communicator(Communicator&&) = delete;
    Communicator &operator=(Communicator&&) = delete;
    Communicator(const Communicator&) = delete;
    Communicator &operator=(const Communicator&) = delete;

    void enablePushChannel(std::function<void(const Answer&)> callback);

    void enableHandshakeAck();

    void setFuseId(std::string fuseId);

    virtual void reply(const Answer &originalMsg, const std::string &module,
                       const google::protobuf::Message &msg);

    // TODO: The module could be an enum (there's a finite number of modules)
    virtual void send(const std::string &module,
                      const google::protobuf::Message &msg);

    template<typename AnswerType = Atom> std::future<std::unique_ptr<Answer>>
    communicateAsync(const std::string &module,
                     const google::protobuf::Message &msg)
    {
        return communicateAsync(module, msg, AnswerType::default_instance());
    }

    template<typename AnswerType = Atom> std::unique_ptr<Answer>
    communicate(const std::string &module,
                const google::protobuf::Message &msg,
                const std::chrono::milliseconds timeout = RECV_TIMEOUT)
    {
        return communicate(module, msg, AnswerType::default_instance(), timeout);
    }

protected:
    virtual std::future<std::unique_ptr<Answer>>
    communicateAsync(const std::string &module,
                     const google::protobuf::Message &msg,
                     const google::protobuf::Message &ans);

    virtual std::unique_ptr<Answer>
    communicate(const std::string &module,
                const google::protobuf::Message &msg,
                const google::protobuf::Message &ans,
                const std::chrono::milliseconds timeout);

private:
    std::pair<std::string, std::string> describe(const google::protobuf::Descriptor &desc) const;
    std::unique_ptr<ClusterMsg> createMessage(const std::string &module,
                                              const bool synchronous,
                                              const google::protobuf::Message &ans,
                                              const google::protobuf::Message &msg) const;
    CommunicationHandler::Pool poolType(const google::protobuf::Message &msg) const;

    const std::string m_uri;
    std::unique_ptr<CommunicationHandler> m_communicationHandler;
    std::string m_fuseId;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATION_COMMUNICATOR_H
