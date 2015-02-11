#!/usr/bin/env python

"""
Prepares a set dockers with oneclients that are configured and ready to start.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function
import argparse
import collections
import copy
import docker
import json
import os
import sys
import time


def parse_config(path):
    with open(path, 'r') as f:
        data = f.read()
        return json.loads(data)


def set_hostname(node, uid):
    parts = list(node.partition('@'))
    parts[2] = '{0}.{1}.dev.docker'.format(parts[0], uid)
    return ''.join(parts)


def tweak_config(config, name, uid):
    cfg = copy.deepcopy(config)
    cfg['nodes'] = {'node': cfg['nodes'][name]}
    node = cfg['nodes']['node']
    node['name'] = set_hostname(node['name'], uid)
    node['op_hostname'] = set_hostname(node['op_hostname'], uid)
    node['gr_hostname'] = set_hostname(node['gr_hostname'], uid)

    return cfg

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Set up dockers with oneclient preconfigured.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/worker',
    help='the image to use for the container',
    dest='image')

parser.add_argument(
    '--bin', '-b',
    action='store',
    default=os.getcwd(),
    help='path to oneclient repository (precompiled)',
    dest='bin')

parser.add_argument(
    '--dns', '-d',
    action='store',
    default='none',
    help='IP address of DNS or "none" - if no dns should be started or ' +
         '"auto" - if it should be started automatically',
    dest='dns')

parser.add_argument(
    '--uid', '-u',
    action='store',
    default=str(int(time.time())),
    help='uid that will be concatenated to docker names',
    dest='uid')

parser.add_argument(
    'config_path',
    action='store',
    help='path to gen_dev_args.json that will be used to configure oneclient instances')


args = parser.parse_args()
uid = args.uid

config = parse_config(args.config_path)['appmock']
config['config']['target_dir'] = '/root/bin'
configs = [tweak_config(config, node, uid) for node in config['nodes']]



envs = {}
volumes = ['/root/bin']
binds = {args.bin: {'bind': '/root/bin', 'ro': True}}
command = \
'''cp -f /tmp/cert /root/cert
cp -f /tmp/key /root/key
chown -f root:root /root/cert /root/key
/root/bin/release/oneclient -f'''

if args.provider_host:
    envs['PROVIDER_HOSTNAME'] = args.provider_host

if args.gr_host:
    envs['GLOBAL_REGISTRY_URL'] = args.gr_host

if args.cert:
    envs['X509_USER_CERT'] = '/root/cert'
    binds[args.cert] = {'bind': '/tmp/cert', 'ro': True}
    volumes.append('/tmp/cert')

if args.key:
    envs['X509_USER_KEY'] = '/root/key'
    binds[args.key] = {'bind': '/tmp/key', 'ro': True}
    volumes.append('/tmp/key')
else:
    envs['X509_USER_KEY'] = '/root/cert'

if args.token:
    command = '''{cmd} -authentication token <<"EOF"
{token}
EOF'''.format(cmd=command,
              token=args.token)

container = client.create_container(
    image=args.image,
    detach=True,
    environment=envs,
    working_dir='/root/bin',
    volumes=volumes,
    command=['sh', '-c', command])

client.start(
    container=container,
    binds=binds)
