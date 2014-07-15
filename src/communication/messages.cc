/**
 * @file messages.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/messages.h"

#include "make_unique.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <google/protobuf/descriptor.h>

namespace
{
std::string lower(std::string what)
{
    boost::algorithm::to_lower(what);
    return std::move(what);
}

template<typename AnswerType = veil::communication::messages::Atom>
std::unique_ptr<veil::communication::messages::ClusterMsg>
createMsg(std::string moduleName, std::string decoderName,
          std::string answerDecoderName, bool synchronous,
          const google::protobuf::Message &inputMessage)
{
    auto msg = std::make_unique<veil::communication::messages::ClusterMsg>();

    msg->set_module_name(lower(std::move(moduleName)));
    msg->set_protocol_version(veil::communication::messages::PROTOCOL_VERSION);
    msg->set_message_type(lower(inputMessage.GetDescriptor()->name()));
    msg->set_message_decoder_name(lower(std::move(decoderName)));
    msg->set_answer_type(lower(AnswerType::descriptor()->name()));
    msg->set_answer_decoder_name(lower(std::move(answerDecoderName)));
    msg->set_synch(synchronous);

    inputMessage.SerializeToString(msg->mutable_input());

    return std::move(msg);
}
}

namespace veil
{
namespace communication
{
namespace messages
{

std::unique_ptr<ClusterMsg> create(const ChannelRegistration &msg)
{
    return createMsg(
        FSLOGIC_MODULE_NAME,
        FUSE_MESSAGES_DECODER,
        COMMUNICATION_PROTOCOL_DECODER,
        false,
        msg
    );
}

std::unique_ptr<ClusterMsg> create(const HandshakeAck &msg)
{
    return createMsg(
        "",
        FUSE_MESSAGES_DECODER,
        COMMUNICATION_PROTOCOL_DECODER,
        true,
        msg
    );
}

} // namespace messages
} // namespace communication
} // namespace veil
