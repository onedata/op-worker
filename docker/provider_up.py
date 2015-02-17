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
import sys
import time

try:
    import xml.etree.cElementTree as eTree
except ImportError:
    import xml.etree.ElementTree as eTree

try:  # Python 2
    from urllib2 import urlopen
    from urllib2 import URLError
    from httplib import BadStatusLine
except ImportError:  # Python 3
    from urllib.request import urlopen
    from urllib.error import URLError
    from http.client import BadStatusLine


def tweak_config(config, name, uid):
    cfg = copy.deepcopy(config)
    cfg['nodes'] = {'node': cfg['nodes'][name]}

    sys_config = cfg['nodes']['node']['sys.config']
    sys_config['ccm_nodes'] = [common.format_hostname(n, uid) for n in sys_config['ccm_nodes']]

    vm_args = cfg['nodes']['node']['vm.args']
    vm_args['name'] = common.format_hostname(vm_args['name'], uid)

    return cfg


def is_up(ip):
    url = 'https://{0}/nagios'.format(ip)
    try:
        fo = urlopen(url, timeout=1)
        tree = eTree.parse(fo)
        healthdata = tree.getroot()
        status = healthdata.attrib['status']
        return status == 'ok'
    except URLError:
        return False
    except BadStatusLine:
        return False


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
    default=common.generate_uid(),
    help='uid that will be concatenated to docker names',
    dest='uid')

parser.add_argument(
    'config_path',
    action='store',
    help='path to gen_dev_args.json that will be used to configure ' +
         'oneprovider_node instances')

args = parser.parse_args()
uid = args.uid

config = common.parse_json_file(args.config_path)['oneprovider_node']
config['config']['target_dir'] = '/root/bin'
configs = [tweak_config(config, node, uid) for node in config['nodes']]

output = collections.defaultdict(list)

(dns_servers, dns_output) = common.set_up_dns(args.dns, uid)
common.merge(output, dns_output)

workers = []
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
        name=common.format_dockername(name, uid),
        volumes=[(args.bin, '/root/build', 'ro')],
        dns_list=dns_servers,
        command=command)

    if node_type == 'worker':
        workers.append(container)

    output['docker_ids'].append(container)
    output['op_{0}_nodes'.format(node_type)].append(node_name)

worker_ip = docker.inspect(workers[0])['NetworkSettings']['IPAddress']
deadline = time.time() + 60
while not is_up(worker_ip):
    if time.time() > deadline:
        print('WARNING: did not get "ok" healthcheck status for 1 minute',
              file=sys.stderr)
        break

    time.sleep(1)

# Print JSON to output so it can be parsed by other scripts
print(json.dumps(output))
