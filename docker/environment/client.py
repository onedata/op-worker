# coding=utf-8
"""Authors: Łukasz Opioła, Konrad Zemek
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Prepares a set dockers with oneclient instances that are configured and ready
to start.
"""

import copy
import os
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
        client_config['mounting_point'] = client['mounting_point']
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

    # mount oneclients
    for client in node["clients"]:
        name = client["name"]
        op = client["op_domain"]
        gr = client["gr_domain"]
        key = client["user_key"]
        mounting_point = client["mounting_point"]

        key_file_path = os.path.join(common.get_file_dir(config_path), key)
        command = '''cat <<"EOF" > /tmp/{user}_key
{key_file}
EOF
chmod 600 /tmp/{user}_key'''
        command = command.format(
            user=name,
            key_file=open(key_file_path, 'r').read())
        assert 0 is docker.exec_(container, command)

        command = ['mkdir', '-p', mounting_point]
        assert 0 is docker.exec_(container, command)

        if 'token' in client:
            user = client['token']
            # TODO: gen_token
            command = '''echo \"#!/usr/bin/env escript
%%! -name gettoken@test -setcookie cookie3

main([GR_Node, UID]) ->
  try
    Token = rpc:call(list_to_atom(GR_Node), auth_logic, gen_auth_code, [list_to_binary(UID)]),
    {ok, File} = file:open("token", [write]),
    ok = file:write(File, Token),
    ok = file:close(File)
  catch
      _T:M -> io:format(\\\"ERROR: ~p~n\\\", [M])
  end.\" > get_token.escript && chmod u+x get_token.escript'''
            assert 0 is docker.exec_(container, command)
            gr_node = "gr@node1." + gr
            command = "docker exec %s bash -c './get_token.escript %s %s'" % (container, gr_node, user)
            assert 0 is subprocess.call([command], shell=True)

            gr = "node1." + gr
            op = "worker1." + op
            command = "GLOBAL_REGISTRY_URL=" + gr + \
                      " PROVIDER_HOSTNAME=" + op + \
                      ' ./oneclient --authentication token --no_check_certificate ' + mounting_point + \
                  ' < token'
            # TODO: uncomment when mounting will succeed
            # assert 0 is docker.exec_(container, command)
        elif 'user_cert' in client:
            # TODO: uncomment when we'll respect certificates
            # cert = client["user_cert"]
            # cert_file_path = os.path.join(common.get_file_dir(config_path), cert)
            # command = '''cat <<"EOF" > /tmp/{user}_cert
            # {cert_file}
            # EOF
            # chmod 600 /tmp/{user}_cert'''
            # command = command.format(
            #     user=name,
            #     cert_file=open(cert_file_path, 'r').read())
            # assert 0 is docker.exec_(container, command)

            # command = '''X509_USER_CERT=/tmp/%s_cert X509_USER_KEY=/tmp/%s_key PROVIDER_HOSTNAME=%s GLOBAL_REGISTRY_URL=%s ./oneclient %s''' % (name, name, op, gr, mounting_point)
            # docker.exec_(container, command, output=True)
            pass

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
