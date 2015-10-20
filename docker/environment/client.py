# coding=utf-8
"""Authors: Łukasz Opioła, Konrad Zemek
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


def _tweak_config(config, sys_config, name, uid):
    # print(config)
    cfg = copy.deepcopy(config)
    cfg = {'node': cfg[name]}
    node = cfg['node']
    sys_config_name = cfg['node']['os_config']
    cfg['sys_config'] = sys_config[sys_config_name]
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

        # print "############## CLIENT #################"
        # print client_config
        node['clients'].append(client_config)
    # print "############# CFG ##############"
    # print cfg
    return cfg


def _node_up(image, bindir, config, config_path, dns_servers):
    print("### CONFIG ###")
    print(config)
    node = config['node']
    hostname = node['name']
    sys_config = config['sys_config']
    print hostname

    # @doc obsolete way of defining clients in env.json
    #
    # cert_file_path = node['user_cert']
    # key_file_path = node['user_key']
    # # cert_file_path and key_file_path can both be an absolute path
    # # or relative to gen_dev_args.json
    # cert_file_path = os.path.join(common.get_file_dir(config_path),
    #                               cert_file_path)
    # key_file_path = os.path.join(common.get_file_dir(config_path),
    #                              key_file_path)
    #
    # node['user_cert'] = '/tmp/cert'
    # node['user_key'] = '/tmp/key'
    #
    # envs = {'X509_USER_CERT': node['user_cert'],
    #         'X509_USER_KEY': node['user_key'],
    #         'PROVIDER_HOSTNAME': node['op_domain'],
    #         'GLOBAL_REGISTRY_URL': node['gr_domain']}

    # We want the binary from debug more than relwithdebinfo, and any of these
    # more than from release (ifs are in reverse order so it works when
    # there are multiple dirs).

    command = '''set -e
[ -d /root/build/release ] && cp /root/build/release/oneclient /root/bin/oneclient
[ -d /root/build/relwithdebinfo ] && cp /root/build/relwithdebinfo/oneclient /root/bin/oneclient
[ -d /root/build/debug ] && cp /root/build/debug/oneclient /root/bin/oneclient
bash'''

    volumes = [(bindir, '/root/build', 'ro')]
    for s in sys_config['storages']:
        # print s
        volumes.append(('/tmp/onedata/storage/'+s, s, 'rw'))

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

    for user in sys_config['users']:
        uid = str(hash(user) % 50000 + 10000)
        command = "docker exec %s adduser --disabled-password --gecos '' --uid %s %s" % (container, uid, user)
        subprocess.check_call(command, stdout=sys.stdout, shell=True)

    for group in sys_config['groups']:
        gid = str(hash(group) % 50000 + 10000)
        command = "docker exec %s groupadd -g %s %s" % (container, gid, group)
        subprocess.check_call(command, stdout=sys.stdout, shell=True)
        for user in sys_config['groups'][group]:
            command = "docker exec %s usermod -a -G %s %s" % (container, group, user)
            subprocess.check_call(command, stdout=sys.stdout, shell=True)


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
            # cert_file=open(cert_file_path, 'r').read(),
            key_file=open(key_file_path, 'r').read())
        assert 0 is docker.exec_(container, command)

        command = ['mkdir', '-p', mounting_point]
        assert 0 is docker.exec_(container, command)

        if 'token' in client:
            user = client['token']
            print("##### PWD #####")
            print(subprocess.check_output(['pwd']))
            print(gr)
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
            # command = ['./bamboos/docker/environment/get_token.escript', "gr@node1"+gr, user]
            print(command)
            assert 0 is docker.exec_(container, command)
            gr_node = "gr@node1." + gr
            command = "docker exec %s bash -c './get_token.escript %s %s'" % (container, gr_node, user)
            print(command)
            subprocess.call([command], shell=True)
            #
            # print("### TOKEN ###")
            # print(token)

            # command = 'echo %s > token' % token
            # print(command)
            # assert 0 is docker.exec_(container, command)

            gr = "node1." + gr
            op = "worker1." + op

# " GLOBAL_REGISTRY_URL=" + gr + \
# " PROVIDER_HOSTNAME=" + op + \
# " USER_KEY=/tmp/" + user + "_key" + \
#  \
            command = './oneclient --authentication token --no_check_certificate ' + mounting_point + \
                  ' < token'
            print(command)
            # assert 0 is docker.exec_(container, command)
        elif 'user_cert' in client:
            cert = client["user_cert"]
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
    # print "########## CONFIG ################"
    # print config
    sys_config = common.parse_json_file(config_path)['os_configs']
    configs = [_tweak_config(config, sys_config, node, uid) for node in config]
    # print "########## CONFIGs ################"
    # print configs

    dns_servers, output = dns.maybe_start(dns_server, uid)

    for cfg in configs:
        node_out = _node_up(image, bindir, cfg, config_path, dns_servers)
        common.merge(output, node_out)

    return output
