/**
 * @file communicator.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicator.h"

#include "communication/connection.h"
#include "communication/websocketConnectionPool.h"
#include "make_unique.h"

namespace veil
{
namespace communication
{

Communicator::Communicator(const unsigned int dataConnectionsNumber,
                           const unsigned int metaConnectionsNumber,
                           std::shared_ptr<const CertificateData> certificateData,
                           const bool verifyServerCertificate)
    : m_uri{"wss://whatever"}
    , m_communicationHandler{
        std::make_unique<WebsocketConnectionPool>(
              dataConnectionsNumber, m_uri, certificateData, verifyServerCertificate),
        std::make_unique<WebsocketConnectionPool>(
              metaConnectionsNumber, m_uri, certificateData, verifyServerCertificate)}
{

}

} // namespace communication
} // namespace veil
