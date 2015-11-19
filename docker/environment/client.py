# coding=utf-8
"""Authors: Łukasz Opioła, Konrad Zemek, Piotr Ociepka
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Prepares a set dockers with oneclient instances that are configured and ready
to start.
"""

import copy
import os
import sys
import subprocess

from . import common, docker, dns, globalregistry, provider_worker


def client_hostname(node_name, uid):
    """Formats hostname for a docker hosting oneclient.
    NOTE: Hostnames are also used as docker names!
    """
    return common.format_hostname(node_name, uid)


def _tweak_config(config, os_config, name, uid):
    cfg = copy.deepcopy(config)
    cfg = {'node': cfg[name]}
    node = cfg['node']
    os_config_name = cfg['node']['os_config']
    cfg['os_config'] = os_config[os_config_name]
    node['name'] = client_hostname(name, uid)
    node['clients'] = []
    clients = config[name]['clients']
    for cl in clients:
        client = copy.deepcopy(clients[cl])
        client_config = {}
        client_config['name'] = client['name']
        client_config['op_domain'] = provider_worker.provider_domain(client['op_domain'], uid)
        client_config['gr_domain'] = globalregistry.gr_domain(client['gr_domain'], uid)
        client_config['user_key'] = client['user_key']
        if 'user_cert' in client.keys():
            client_config['user_cert'] = client['user_cert']
        if 'token' in client.keys():
            client_config['token'] = client['token']

        node['clients'].append(client_config)
    return cfg


def _node_up(image, bindir, config, config_path, dns_servers):
    node = config['node']
    hostname = node['name']
    os_config = config['os_config']

    # copy get_token.escript to /root/build
    # local_path = os.getcwd()
    # local_path = os.path.join(local_path, "bamboos", "docker", "environment", "get_token.escript")
    # docker_path = os.path.join(bindir, "get_token.escript")
    # subprocess.check_call(['cp', local_path, docker_path])

    command = '''set -e
[ -d /root/build/release ] && cp /root/build/release/oneclient /root/bin/oneclient
[ -d /root/build/relwithdebinfo ] && cp /root/build/relwithdebinfo/oneclient /root/bin/oneclient
[ -d /root/build/debug ] && cp /root/build/debug/oneclient /root/bin/oneclient
bash'''

    volumes = [(bindir, '/root/build', 'ro')]
    volumes = common.add_shared_storages(volumes, os_config['storages'])

    container = docker.run(
        image=image,
        name=hostname,
        hostname=hostname,
        detach=True,
        interactive=True,
        tty=True,
        workdir='/root/bin',
        volumes=volumes,
        dns_list=dns_servers,
        run_params=["--privileged"],
        command=command)

    # create system users and groups
    common.create_users(container, os_config)
    common.create_groups(container, os_config)

    return {'docker_ids': [container], 'client_nodes': [hostname]}


def up(image, bindir, dns_server, uid, config_path):
    config = common.parse_json_file(config_path)['oneclient']
    os_config = common.parse_json_file(config_path)['os_configs']
    configs = [_tweak_config(config, os_config, node, uid) for node in config]
    dns_servers, output = dns.maybe_start(dns_server, uid)

    for cfg in configs:
        node_out = _node_up(image, bindir, cfg, config_path, dns_servers)
        common.merge(output, node_out)

    return output
