"""Author: Konrad Zemek, Michal Zmuda
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up a set of worker nodes. They can create separate clusters.
Script is parametrised by worker type related configurator.
"""

import copy
import json
import os
import subprocess
import sys
from . import common, docker, riak, couchbase, dns, provider_ccm

CLUSTER_WAIT_FOR_NAGIOS_SECONDS = 60 * 2
# mounting point for op-worker-node docker
DOCKER_BINDIR_PATH = '/root/build'


def cluster_domain(instance, uid):
    """Formats domain for a cluster."""
    return common.format_hostname(instance, uid)


def worker_hostname(node_name, instance, uid):
    """Formats hostname for a docker hosting cluster_worker.
    NOTE: Hostnames are also used as docker names!
    """
    return common.format_hostname([node_name, instance], uid)


def worker_erl_node_name(node_name, instance, uid):
    """Formats erlang node name for a vm on cluster_worker docker.
    """
    hostname = worker_hostname(node_name, instance, uid)
    return common.format_erl_node_name('worker', hostname)


def _tweak_config(config, name, instance, uid, configurator):
    cfg = copy.deepcopy(config)
    cfg['nodes'] = {'node': cfg['nodes'][name]}

    sys_config = cfg['nodes']['node']['sys.config']
    sys_config['ccm_nodes'] = [
        provider_ccm.ccm_erl_node_name(n, instance, uid) for n in
        sys_config['ccm_nodes']]
    # Set the cluster domain (needed for nodes to start)
    sys_config[configurator.domain_env_name()] = cluster_domain(instance, uid)

    sys_config['persistence_driver_module'] = _db_driver_module(cfg['db_driver'])

    if 'vm.args' not in cfg['nodes']['node']:
        cfg['nodes']['node']['vm.args'] = {}
    vm_args = cfg['nodes']['node']['vm.args']
    vm_args['name'] = worker_erl_node_name(name, instance, uid)

    cfg = configurator.tweak_config(cfg, uid)
    return cfg, sys_config['db_nodes']


def _node_up(image, bindir, config, dns_servers, db_node_mappings, logdir, configurator):
    node_name = config['nodes']['node']['vm.args']['name']
    db_nodes = config['nodes']['node']['sys.config']['db_nodes']
    for i in range(len(db_nodes)):
        db_nodes[i] = db_node_mappings[db_nodes[i]]

    (name, sep, hostname) = node_name.partition('@')

    command = '''mkdir -p /root/bin/node/log/
echo 'while ((1)); do chown -R {uid}:{gid} /root/bin/node/log; sleep 1; done' > /root/bin/chown_logs.sh
bash /root/bin/chown_logs.sh &
cat <<"EOF" > /tmp/gen_dev_args.json
{gen_dev_args}
EOF
set -e
escript bamboos/gen_dev/gen_dev.escript /tmp/gen_dev_args.json
/root/bin/node/bin/{executable} console'''
    command = command.format(
        gen_dev_args=json.dumps({configurator.app_name(): config}),
        uid=os.geteuid(),
        gid=os.getegid(),
        executable=configurator.app_name()
    )

    volumes = [(bindir, DOCKER_BINDIR_PATH, 'ro')]
    volumes += configurator.extra_volumes(config)

    if logdir:
        logdir = os.path.join(os.path.abspath(logdir), hostname)
        volumes.extend([(logdir, '/root/bin/node/log', 'rw')])

    container = docker.run(
        image=image,
        name=hostname,
        hostname=hostname,
        detach=True,
        interactive=True,
        tty=True,
        workdir=DOCKER_BINDIR_PATH,
        volumes=volumes,
        dns_list=dns_servers,
        command=command)

    # create system users and grous
    common.create_users(container, config['os_config']['users'])
    common.create_groups(container, config['os_config']['groups'])

    return container, {
        'docker_ids': [container],
        configurator.nodes_list_attribute(): [node_name]
    }


def _ready(container):
    ip = docker.inspect(container)['NetworkSettings']['IPAddress']
    return common.nagios_up(ip, port='6666', protocol='http')


def _riak_up(cluster_name, db_nodes, dns_servers, uid):
    db_node_mappings = {}
    for node in db_nodes:
        db_node_mappings[node] = ''

    i = 0
    for node in iter(db_node_mappings.keys()):
        db_node_mappings[node] = riak.config_entry(cluster_name, i, uid)
        i += 1

    if i == 0:
        return db_node_mappings, {}

    [dns] = dns_servers
    riak_output = riak.up('onedata/riak', dns, uid, None, cluster_name, len(db_node_mappings))

    return db_node_mappings, riak_output


def _couchbase_up(cluster_name, db_nodes, dns_servers, uid):
    db_node_mappings = {}
    for node in db_nodes:
        db_node_mappings[node] = ''

    for i, node in enumerate(db_node_mappings):
        db_node_mappings[node] = couchbase.config_entry(cluster_name, i, uid)

    if not db_node_mappings:
        return db_node_mappings, {}

    [dns] = dns_servers
    couchbase_output = couchbase.up('couchbase/server:community-4.0.0', dns, uid, cluster_name, len(db_node_mappings))

    return db_node_mappings, couchbase_output


def _db_driver(config):
    return config['db_driver'] if 'db_driver' in config else 'couchbase'


def _db_driver_module(db_driver):
    return db_driver + "_datastore_driver"


def up(image, bindir, dns_server, uid, config_path, configurator, logdir=None):
    config = common.parse_json_file(config_path)
    input_dir = config['dirs_config'][configurator.app_name()]['input_dir']
    dns_servers, output = dns.maybe_start(dns_server, uid)

    # Workers of every cluster are started together
    for instance in config[configurator.domains_attribute()]:
        os_config = config[configurator.domains_attribute()][instance]['os_config']
        gen_dev_cfg = {
            'config': {
                'input_dir': input_dir,
                'target_dir': '/root/bin'
            },
            'nodes': config[configurator.domains_attribute()][instance][configurator.app_name()],
            'db_driver': _db_driver(config[configurator.domains_attribute()][instance]),
            'os_config': config['os_configs'][os_config]
        }

        # Tweak configs, retrieve lis of riak nodes to start
        configs = []
        all_db_nodes = []

        for worker_node in gen_dev_cfg['nodes']:
            tw_cfg, db_nodes = _tweak_config(gen_dev_cfg, worker_node, instance, uid, configurator)
            configs.append(tw_cfg)
            all_db_nodes.extend(db_nodes)

        db_node_mappings = None
        db_out = None
        db_driver = _db_driver(config[configurator.domains_attribute()][instance])

        # Start db nodes, obtain mappings
        if db_driver == 'riak':
            db_node_mappings, db_out = _riak_up(instance, all_db_nodes, dns_servers, uid)
        elif db_driver == 'couchbase':
            db_node_mappings, db_out = _couchbase_up(instance, all_db_nodes, dns_servers, uid)
        else:
            raise ValueError("Invalid db_driver: {0}".format(db_driver))

        common.merge(output, db_out)

        # Start the workers
        workers = []
        worker_ips = []
        for cfg in configs:
            worker, node_out = _node_up(image, bindir, cfg, dns_servers, db_node_mappings, logdir, configurator)
            workers.append(worker)
            worker_ips.append(common.get_docker_ip(worker))
            common.merge(output, node_out)

        # Wait for all workers to start
        common.wait_until(_ready, workers, CLUSTER_WAIT_FOR_NAGIOS_SECONDS)

        # Add the domain of current clusters
        domains = {
            'domains': {
                cluster_domain(instance, uid): {
                    'ns': worker_ips,
                    'a': []
                }
            }
        }
        common.merge(output, domains)
        configurator.configure_started_instance(bindir, instance, config, os_config, output)

    # Make sure domains are added to the dns server.
    dns.maybe_restart_with_configuration(dns_server, uid, output)
    return output