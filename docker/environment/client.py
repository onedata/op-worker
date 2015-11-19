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
        client = clients[cl]
        client_config = {'name': client['name'],
                         'op_domain': provider_worker.provider_domain(client['op_domain'], uid),
                         'gr_domain': globalregistry.gr_domain(client['gr_domain'], uid),
                         'user_key': client['user_key'],
                         'user_cert': client['user_cert']}

        node['clients'].append(client_config)
    return cfg


def _node_up(image, bindir, config, config_path, dns_servers):
    node = config['node']
    hostname = node['name']
    os_config = config['os_config']

    client_data = {}

    # We want the binary from debug more than relwithdebinfo, and any of these
    # more than from release (ifs are in reverse order so it works when
    # there are multiple dirs).
    command = '''set -e
[ -d /root/build/release ] && cp /root/build/release/oneclient /root/bin/oneclient
[ -d /root/build/relwithdebinfo ] && cp /root/build/relwithdebinfo/oneclient /root/bin/oneclient
[ -d /root/build/debug ] && cp /root/build/debug/oneclient /root/bin/oneclient
mkdir /tmp/certs
mkdir /tmp/keys
'''

    for client in node['clients']:
        # for each client instance we want to have separated certs and keys
        client_name = client["name"]
        client_data[client_name] = {'client_name': client_name,
                                    'op_domain': client['op_domain'],
                                    'gr_domain': client['gr_domain'],
                                    # todo: add this field to env.json to mount oneclient
                                    # 'mounting_path': client['mounting_path],
                                    # 'token_for': client['token_for'],
                                    'cert_file_path': client['user_cert'],
                                    'key_file_path': client['user_key']}
        # cert_file_path and key_file_path can both be an absolute path
        # or relative to gen_dev_args.json
        cert_file_path = os.path.join(common.get_file_dir(config_path),
                                      client_name,
                                      client_data[client_name]['cert_file_path'])
        key_file_path = os.path.join(common.get_file_dir(config_path),
                                     client_name,
                                     client_data[client_name]['key_file_path'])
        command += '''mkdir /tmp/certs/{client_name}
mkdir /tmp/keys/{client_name}
cat <<"EOF" > /tmp/certs/{client_name}/cert
{cert_file}
EOF
cat <<"EOF" > /tmp/keys/{client_name}/key
{key_file}
EOF
'''

        command = command.format(
            client_name=client_name,
            cert_file=open(cert_file_path, 'r').read(),
            key_file=open(key_file_path, 'r').read())
    command += '''bash'''

    volumes = [(bindir, '/root/build', 'ro')]
    volumes += [common.volume_for_storage(s) for s in os_config['storages']]

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
    common.create_users(container, os_config['users'])
    common.create_groups(container, os_config['groups'])

    # mount oneclients as declared in config
    for client in client_data:
        mount_oneclient(container, client_data[client])

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


def mount_oneclient(container, config):
    # todo: uncomment to mount oneclient
    # # ask GR for token with get_token.escript (from onedata/tests/cucumber/scenarios/steps/utils)
    # command = ['escript', 'get_token.escript', config['gr_domain'], config['token_for']]
    # token = docker.exec_(container, command, tty=True, output=True)
    # command = 'echo ' + token + ' > token'
    # docker.exec_(container, command)
    # # mount oneclient
    # command = 'mkdir ' + config['mounting_path'] + \
    #           ' X509_USER_CERT ' + config['cert_file_path'] + \
    #           ' X509_USER_KEY ' + config['key_file_path'] + \
    #           ' PROVIDER_HOSTNAME ' + config['op_domain'] + \
    #           ' GLOBAL_REGISTRY_URL ' + config['gr_domain'] + \
    #           ' ./oneclient --authentication token --no_check_certificate ' + \
    #           config['mounting_path'] + \
    #           ' < token && rm token'
    # docker.exec_(container, command, tty=True)
    pass
