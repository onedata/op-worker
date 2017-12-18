/**
 * @file init.cc
 * @author Bartek Kryza
 * @copyright (C) 2017 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "helpers/init.h"
#include "monitoring/graphiteMetricsCollector.h"
#include "monitoring/metricsCollector.h"
#include "monitoring/monitoring.h"
#include "monitoring/monitoringConfiguration.h"

#include <folly/Singleton.h>

namespace one {
namespace helpers {

using namespace one::monitoring;

void init() { folly::SingletonVault::singleton()->registrationComplete(); }

void configureMonitoring(
    std::shared_ptr<MonitoringConfiguration> conf, bool start)
{
    std::shared_ptr<MetricsCollector> metricsCollector;

    if (dynamic_cast<GraphiteMonitoringConfiguration *>(conf.get())) {
        metricsCollector =
            MetricsCollector::getInstance<GraphiteMetricsCollector>();
    }
    else {
        throw std::runtime_error("Unsupported monitoring type requested.");
    }

    metricsCollector->setConfiguration(conf);

    if (start)
        metricsCollector->start();
}
}
}
