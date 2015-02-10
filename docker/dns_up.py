#!/usr/bin/env python

"""
Brings up a DNS server with container (skydns + skydock) that allow
different dockers to see each other by hostnames.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function

import argparse
import collections
import docker
import json
import os
import time

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Bring up skydns.')

parser.add_argument(
    '--create-service', '-c',
    action='store',
    default='{0}/createService.js'.format(get_script_dir()),
    help='path to createService.js plugin',
    dest='create_service')

parser.add_argument(
    '--uid', '-u',
    action='store',
    default=str(int(time.time())),
    help='uid that will be concatenated to docker name',
    dest='uid')

args = parser.parse_args()
uid = args.uid

skydns = docker.run(
    image='crosbymichael/skydns',
    detach=True,
    name='skydns_{0}'.format(uid),
    command=['-nameserver', '8.8.8.8:53', '-domain', 'docker'])

skydock = docker.run(
    image='crosbymichael/skydock',
    detach=True,
    name='skydock_{0}'.format(uid),
    reflect=[('/var/run/docker.sock', 'rw')],
    volumes=[(args.create_service, '/createService.js', 'ro')],
    command=['-ttl', '30', '-environment', 'dev', '-s', '/var/run/docker.sock',
             '-domain', 'docker', '-name', 'skydns_{0}'.format(uid), '-plugins',
             '/createService.js'])

skydns_config = docker.inspect(skydns)
dns = skydns_config['NetworkSettings']['IPAddress']

output = collections.defaultdict(list)
output['dns'] = dns
output['docker_ids'] = [skydns, skydock]

# Print JSON to output so it can be parsed by other scripts
print(json.dumps(output))