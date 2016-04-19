"""Author: Michal Zmuda
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up a set of cluster-worker nodes. They can create separate clusters.
"""

from . import worker

DOCKER_BINDIR_PATH = '/root/build'


def up(image, bindir, dns_server, uid, config_path, logdir=None,
       storages_dockers=None):
    return worker.up(image, bindir, dns_server, uid, config_path,
                     ClusterWorkerConfigurator(), logdir, storages_dockers)


class ClusterWorkerConfigurator:
    def tweak_config(self, cfg, uid, instance):
        return cfg

    def pre_start_commands(self, domain):
        return 'escript bamboos/gen_dev/gen_dev.escript /tmp/gen_dev_args.json'

    def pre_configure_instance(self, instance, uid, config):
        pass

    # Called AFTER the instance (cluster of workers) has been started
    def post_configure_instance(self, bindir, instance, config, container_ids,
                                output, storages_dockers):
        pass

    def extra_volumes(self, config, bindir, instance):
        return []

    def app_name(self):
        return "cluster_worker"

    def domains_attribute(self):
        return "cluster_domains"

    def domain_env_name(self):
        return "cluster_domain"

    def nodes_list_attribute(self):
        return "cluster_worker_nodes"
