#!/usr/bin/env python

"""
Brings up a set of appmock instances.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function

import argparse
import collections
import copy
import docker
import json
import os
import time
import subprocess


def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))


def get_file_dir(filepath):
    return os.path.dirname(os.path.realpath(filepath))


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

    vm_args = cfg['nodes']['node']['vm.args']
    vm_args['name'] = set_hostname(vm_args['name'], uid)

    return cfg


def run_command(cmd):
    return subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.PIPE).communicate()[0]


parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Bring up appmock nodes.')

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
    help='the path to appmock repository (precompiled)',
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
    help='path to gen_dev_args.json that will be used to configure appmock instances')


args = parser.parse_args()
uid = args.uid

config = parse_config(args.config_path)['appmock']
config['config']['target_dir'] = '/root/bin'
configs = [tweak_config(config, node, uid) for node in config['nodes']]

output = collections.defaultdict(list)

dns = args.dns
if dns == 'auto':
    dns_config = json.loads(run_command([get_script_dir() + '/dns_up.py', '--uid', uid]))
    dns = dns_config['dns']
    output['dns'] = dns_config['dns']
    output['docker_ids'] = dns_config['docker_ids']
elif dns == 'none':
    dns = None

for cfg in configs:
    node_name = cfg['nodes']['node']['vm.args']['name']

    (name, sep, hostname) = node_name.partition('@')

    sys_config = cfg['nodes']['node']['sys.config']
    app_desc_file_path = sys_config['app_description_file']
    app_desc_file_name = os.path.basename(app_desc_file_path)
    # App desc file can be an absolute path or relative to gen_dev_args.json
    if not os.path.isabs(app_desc_file_path):
        app_desc_file_path = get_file_dir(args.config_path) + '/' + app_desc_file_path
    sys_config['app_description_file'] = '/tmp/' + app_desc_file_name

    command = \
    '''set -e
cat <<"EOF" > /tmp/{app_desc_file_name}
{app_desc_file}
EOF
cat <<"EOF" > /tmp/gen_dev_args.json
{gen_dev_args}
EOF
escript bamboos/gen_dev/gen_dev.escript /tmp/gen_dev_args.json
/root/bin/node/bin/appmock console'''
    command = command.format(
        app_desc_file_name=app_desc_file_name,
        app_desc_file=open(app_desc_file_path, 'r').read(),
        gen_dev_args=json.dumps({'appmock': cfg}))

    if dns is None:
        container = docker.run(
            image=args.image,
            hostname=hostname,
            detach=True,
            interactive=True,
            tty=True,
            workdir='/root/build',
            name='{0}_{1}'.format(name, uid),
            volumes=[(args.bin, '/root/build', 'ro')],
            command=command)
    else:
        container = docker.run(
            image=args.image,
            hostname=hostname,
            detach=True,
            interactive=True,
            tty=True,
            workdir='/root/build',
            name='{0}_{1}'.format(name, uid),
            volumes=[(args.bin, '/root/build', 'ro')],
            dns=[dns],
            command=command)

    output['docker_ids'].append(container)
    output['appmock_nodes'].append(node_name)

# Print JSON to output so it can be parsed by other scripts
print(json.dumps(output))






