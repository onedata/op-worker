/**
 * @file communicationHandler.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicationHandler.h"

#include "communication/connection.h"
#include "communication/connectionPool.h"

namespace veil
{
namespace communication
{

CommunicationHandler::CommunicationHandler(std::unique_ptr<ConnectionPool> dataPool,
                                           std::unique_ptr<ConnectionPool> metaPool)
    : m_dataPool{std::move(dataPool)}
    , m_metaPool{std::move(metaPool)}
{

}

std::future<CommunicationHandler::Answer> CommunicationHandler::send()
{
    return {};
}

void CommunicationHandler::subscribe(std::function<bool(const Answer&)> predicate,
                                     std::function<bool(const Answer&)> callback)
{

}

void CommunicationHandler::setFuseId(std::string fuseId)
{
    m_fuseId = std::move(fuseId);
}

void CommunicationHandler::registerPushChannels()
{

}

} // namespace communication
} // namespace veil
