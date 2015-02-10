#!/usr/bin/env python

"""
Brings up a DNS server with container (skydns + skydock) that allow
different dockers to see each other by hostnames.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function

import argparse
import collections
import json
import os
import time
import subprocess


def parse_config(path):
    with open(path, 'r') as f:
        data = f.read()
        return json.loads(data)


def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))


def run_command(cmd):
    return subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.PIPE).communicate()[0]


parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Bring up onedata environment.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/worker',
    help='the image to use for the components',
    dest='image')

parser.add_argument(
    '--bin-provider', '-bp',
    action='store',
    default=os.getcwd() + '/oneprovider',
    help='the path to oneprovider repository (precompiled)',
    dest='bin_op')

parser.add_argument(
    '--bin-gr', '-bg',
    action='store',
    default=os.getcwd() + '/globalregistry',
    help='the path to globalregistry repository (precompiled)',
    dest='bin_gr')

parser.add_argument(
    '--bin-appmock', '-ba',
    action='store',
    default=os.getcwd() + '/appmock',
    help='the path to appmock repository (precompiled)',
    dest='bin_am')

parser.add_argument(
    'config_path',
    action='store',
    help='path to gen_dev_args.json file that will be used to configure the environment')

args = parser.parse_args()
config = parse_config(args.config_path)
uid = str(int(time.time()))

# Start DNS
dns_output = json.loads(run_command([get_script_dir() + '/dns_up.py', '--uid', uid]))
dns = dns_output['dns']

# Start globalregistry instances
gr_output = collections.defaultdict(list)
gr_output['docker_ids'] = []
gr_output['gr_nodes'] = []
gr_output['gr_db_nodes'] = []
if 'globalregistry' in config:
    gr_output = json.loads(run_command(
        [get_script_dir() + '/globalregistry_up.py',
         '--image', args.image,
         '--bin', args.bin_gr,
         '--dns', dns,
         '--uid', uid,
         args.config_path]))

# Start oneprovider_node instances
op_output = collections.defaultdict(list)
op_output['docker_ids'] = []
op_output['op_ccm_nodes'] = []
op_output['op_worker_nodes'] = []
if 'oneprovider_node' in config:
    op_output = json.loads(run_command(
        [get_script_dir() + '/provider_up.py',
         '--image', args.image,
         '--bin', args.bin_op,
         '--dns', dns,
         '--uid', uid,
         args.config_path]))

# Start appmock instances
am_output = collections.defaultdict(list)
am_output['docker_ids'] = []
am_output['appmock_nodes'] = []
if 'appmock' in config:
    am_output = json.loads(run_command(
        [get_script_dir() + '/appmock_up.py',
         '--image', args.image,
         '--bin', args.bin_am,
         '--dns', dns,
         '--uid', uid,
         args.config_path]))

# Gather output from all components' starting and print it
output = collections.defaultdict(list)
output['dns'] = dns_output['dns']
output['docker_ids'] = dns_output['docker_ids'] + gr_output['docker_ids'] + op_output['docker_ids'] + am_output['docker_ids']
output['gr_nodes'] = gr_output['gr_nodes']
output['gr_db_nodes'] = gr_output['gr_db_nodes']
output['op_ccm_nodes'] = op_output['op_ccm_nodes']
output['op_worker_nodes'] = op_output['op_worker_nodes']
output['appmock_nodes'] = am_output['appmock_nodes']
print(json.dumps(output))
