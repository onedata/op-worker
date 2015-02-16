#!/usr/bin/env python

"""
Brings up a riak cluster.
Run the script with -h flag to learn about script's running options.
"""

import argparse
import docker
import os
import time

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Bring up oneprovider cluster.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/riak',
    help='container image of riak',
    dest='image')

parser.add_argument(
    '--maps',
    action='store',
    default='{"props":{"n_val":2, "datatype":"map"}}',
    help='argument for `riak-admin create maps`',
    dest='maps')

parser.add_argument(
    '--nodes', '-n',
    type=int,
    action='store',
    default=2,
    help='number of riak nodes to bring up',
    dest='nodes')

args = parser.parse_args()
uid = str(int(time.time()))
create_service = '{0}/createService.js'.format(os.path.dirname(os.path.realpath(__file__)))

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
    volumes=[(create_service, '/createService.js', 'ro')],
    command=['-ttl', '30', '-environment', 'dev', '-s', '/var/run/docker.sock',
             '-domain', 'docker', '-name', 'skydns_{0}'.format(uid), '-plugins',
             '/createService.js'])

skydns_config = docker.inspect(skydns)
dns = skydns_config['NetworkSettings']['IPAddress']

command = '''
sed -i 's/riak@127.0.0.1/riak@{hostname}/' /etc/riak/riak.conf
sed -i 's/127.0.0.1:/0.0.0.0:/' /etc/riak/riak.conf
riak start
riak-admin bucket-type create maps '{maps}'
riak-admin bucket-type activate maps
riak attach
'''

nodes = []
for i in range(args.nodes):
    hostname = 'riak{0}.{1}.dev.docker'.format(i, uid)
    node = docker.run(
        image=args.image,
        name='riak{0}_{1}'.format(i, uid),
        hostname=hostname,
        detach=True,
        interactive=True,
        tty=True,
        dns=[dns],
        command=command.format(maps=args.maps, hostname=hostname))

    nodes.append((node, hostname))

master_node, master_hostname = nodes[0]

for node, _ in nodes:
    while docker.exec_(node, ['riak', 'ping']) != 0:
        time.sleep(1)

if len(nodes) > 1:
    for node, _ in nodes[1:]:
        docker.exec_(
            node,
            ['riak-admin', 'cluster', 'join', 'riak@{0}'.format(master_hostname)])

    docker.exec_(master_node, ['riak-admin', 'cluster', 'plan'])
    docker.exec_(master_node, ['riak-admin', 'cluster', 'commit'])
