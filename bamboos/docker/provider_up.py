#!/usr/bin/env python

"""
Brings up a set of oneprovider nodes. They can create separate clusters.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function

import argparse
import collections
import common
import copy
import docker
import json
import os
import time


def tweak_config(config, name, uid):
    cfg = copy.deepcopy(config)
    cfg['nodes'] = {'node': cfg['nodes'][name]}

    sys_config = cfg['nodes']['node']['sys.config']
    sys_config['ccm_nodes'] = [common.format_hostname(n, uid) for n in sys_config['ccm_nodes']]

    vm_args = cfg['nodes']['node']['vm.args']
    vm_args['name'] = common.format_hostname(vm_args['name'], uid)

    return cfg


parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Bring up oneprovider nodes.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/worker',
    help='docker image to use as a container',
    dest='image')

parser.add_argument(
    '--bin', '-b',
    action='store',
    default=os.getcwd(),
    help='path to oneprovider directory (precompiled)',
    dest='bin')

parser.add_argument(
    '--dns', '-d',
    action='store',
    default='auto',
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
    help='path to gen_dev_args.json that will be used to configure oneprovider_node instances')

args = parser.parse_args()
uid = args.uid

config = common.parse_json_file(args.config_path)['oneprovider_node']
config['config']['target_dir'] = '/root/bin'
configs = [tweak_config(config, node, uid) for node in config['nodes']]

output = collections.defaultdict(list)

dns = args.dns
if dns == 'auto':
    dns_config = common.run_script_return_dict('dns_up.py', ['--uid', uid])
    dns = dns_config['dns']
    output['dns'] = dns_config['dns']
    output['docker_ids'] = dns_config['docker_ids']
elif dns == 'none':
    dns = None

for cfg in configs:
    node_type = cfg['nodes']['node']['sys.config']['node_type']
    node_name = cfg['nodes']['node']['vm.args']['name']

    (name, sep, hostname) = node_name.partition('@')

    command = \
        '''set -e
cat <<"EOF" > /tmp/gen_dev_args.json
{gen_dev_args}
EOF
escript bamboos/gen_dev/gen_dev.escript /tmp/gen_dev_args.json
/root/bin/node/bin/oneprovider_node console'''
    command = command.format(gen_dev_args=json.dumps({'oneprovider_node': cfg}))

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
    output['op_{type}_nodes'.format(type=node_type)].append(node_name)

# Print JSON to output so it can be parsed by other scripts
print(json.dumps(output))
