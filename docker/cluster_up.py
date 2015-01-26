#!/usr/bin/env python

import argparse
import sys
import os
import uuid
import re
import time
import docker
import json
import copy

def parse_config(path):
  with open(path, 'r') as f:
    data = f.read()
    return json.loads(data)

def set_hostname(node, cookie):
  parts = list(node.partition('@'))
  parts[2] = '{name}.{cookie}.dev.docker'.format(name=parts[0], cookie=cookie)
  return ''.join(parts)

def tweak_config(config, name):
  cfg = copy.deepcopy(config)
  cfg['nodes'] = { 'node': cfg['nodes'][name] }

  sys_config = cfg['nodes']['node']['sys.config']
  sys_config['ccm_nodes'] = [set_hostname(n, cookie) for n in sys_config['ccm_nodes']]
  sys_config['db_nodes']  = [set_hostname(n, cookie) for n in sys_config['db_nodes']]

  vm_args = cfg['nodes']['node']['vm.args']
  vm_args['cookie'] = cookie
  vm_args['name'] = set_hostname(vm_args['name'], cookie)

  return cfg

COMMAND = re.sub(r'\s+', ' ',
  '''echo "{config}" > /tmp/gen_dev.args &&
     cd /root/build &&
     escript gen_dev.erl /tmp/gen_dev.args &&
     /root/bin/node/bin/oneprovider_node foreground''')

parser = argparse.ArgumentParser(
  formatter_class=argparse.ArgumentDefaultsHelpFormatter,
  description='Bring up onedata cluster.')

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
  help='the path to onedata repository (precompiled)',
  dest='bin')

parser.add_argument(
  'config_path',
  action='store',
  help='path to gen_dev_args.json that will be used to configure the cluster')

args = parser.parse_args()
client = docker.Client()
cookie = str(int(time.time()))

config = parse_config(args.config_path)
config['config']['target_dir'] = '/root/bin'
configs = [tweak_config(config, node) for node in config['nodes']]

skydns = client.create_container(image='crosbymichael/skydns',
                                 detach=True,
                                 name='skydns_'+cookie,
                                 command='-nameserver 8.8.8.8:53 -domain docker')
client.start(container=skydns)

createServicePath = os.path.dirname(os.path.realpath(__file__)) + '/createService.js'

skydock = client.create_container(image='crosbymichael/skydock',
                                  detach=True,
                                  name='skydock_'+cookie,
                                  volumes=['/myplugins.js', '/docker.sock'],
                                  command='-ttl 30 -environment dev -s /docker.sock -domain docker -name skydns_'+cookie+' -plugins /myplugins.js')

client.start(container=skydock,
             binds={ createServicePath: { 'bind': '/myplugins.js', 'ro': True },
                     '/var/run/docker.sock': { 'bind': '/docker.sock', 'ro': False } })

config = client.inspect_container(skydns)
dns = config['NetworkSettings']['IPAddress']

for cfg in configs:
  (name, sep, hostname) = cfg['nodes']['node']['vm.args']['name'].partition('@')
  container = client.create_container(
    image=args.image,
    command=['bash', '-c', COMMAND.format(config=json.dumps(cfg).replace('"', r'\"'))],
    hostname= hostname,
    detach=True,
    name='{name}_{cookie}'.format(name=name, cookie=cookie),
    volumes='/root/build')

  client.start(
    container=container,
    binds={ args.bin: { 'bind': '/root/build', 'ro': True } },
    dns=[dns])
