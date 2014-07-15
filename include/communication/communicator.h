/**
 * @file communicator.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef VEILHELPERS_COMMUNICATOR_H
#define VEILHELPERS_COMMUNICATOR_H


#include "communication/communicationHandler.h"

#include <memory>
#include <string>

namespace veil
{
namespace communication
{

class CertificateData;

class Communicator
{
public:
    Communicator(const unsigned int dataConnectionsNumber,
                 const unsigned int metaConnectionsNumber,
                 std::shared_ptr<const CertificateData> certificateData,
                 const bool verifyServerCertificate);

private:
    void registerPushChannel(); // add a callback to be called in Pool on Connection::onOpen?

    const std::string m_uri;
    CommunicationHandler m_communicationHandler;
};

} // namespace communication
} // namespace veil


#endif // VEILHELPERS_COMMUNICATOR_H
