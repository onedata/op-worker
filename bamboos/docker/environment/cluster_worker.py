"""Author: Michal Zmuda
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up a set of onecluster worker nodes. They can create separate clusters.
"""

import os
import subprocess
import sys
from . import common, docker, any_worker, globalregistry

DOCKER_BINDIR_PATH = '/root/build'

def up(image, bindir, dns_server, uid, config_path, logdir=None):
    return any_worker.up(image, bindir, dns_server, uid, config_path, ClusterWorkerConfigurator(), logdir)


class ClusterWorkerConfigurator:
    def tweak_config(self, cfg, uid):
        return cfg

    def configure_started_instance(self, bindir, instance, config, os_config, output):
        pass

    def tweak_run_parameters(self, config, volumes):
        return volumes

    def app_name(self):
        return "cluster_worker"

    def domains_attribute(self):
        return "cluster_domains"

    def domain_env_name(self):
        return "cluster_domain"

    def nodes_list_attribute(self):
        return "cluster_worker_nodes"