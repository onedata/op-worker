#!/usr/bin/env python

import argparse
import collections
import copy
import docker
import json
import os
import time

def parse_config(path):
  with open(path, 'r') as f:
    data = f.read()
    return json.loads(data)

def set_hostname(node, uid):
  parts = list(node.partition('@'))
  parts[2] = '{name}.{uid}.dev.docker'.format(name=parts[0], uid=uid)
  return ''.join(parts)

def tweak_config(config, name, uid):
  cfg = copy.deepcopy(config)
  cfg['nodes'] = { 'node': cfg['nodes'][name] }

  sys_config = cfg['nodes']['node']['sys.config']
  sys_config['ccm_nodes'] = [set_hostname(n, uid) for n in sys_config['ccm_nodes']]
  sys_config['db_nodes']  = [set_hostname(n, uid) for n in sys_config['db_nodes']]

  vm_args = cfg['nodes']['node']['vm.args']
  vm_args['name'] = set_hostname(vm_args['name'], uid)

  return cfg

def pull_image(name):
  try:
    client.inspect_image(name)
  except docker.errors.APIError:
    print('Pulling image {name}'.format(name=name))
    client.pull(name)


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
  '--create-service', '-c',
  action='store',
  default='{dir}/createService.js'.format(dir=os.path.dirname(os.path.realpath(__file__))),
  help='the path to createService.js plugin',
  dest='create_service')

parser.add_argument(
  'config_path',
  action='store',
  help='path to gen_dev_args.json that will be used to configure the cluster')

args = parser.parse_args()
client = docker.Client()
uid = str(int(time.time()))

config = parse_config(args.config_path)
config['config']['target_dir'] = '/root/bin'
configs = [tweak_config(config, node, uid) for node in config['nodes']]

pull_image(args.image)
pull_image('crosbymichael/skydns')
pull_image('crosbymichael/skydock')

skydns = client.create_container(
  image='crosbymichael/skydns',
  detach=True,
  name='skydns_{uid}'.format(uid=uid),
  command='-nameserver 8.8.8.8:53 -domain docker')
client.start(container=skydns)

skydock = client.create_container(
  image='crosbymichael/skydock',
  detach=True,
  name='skydock_{uid}'.format(uid=uid),
  volumes=['/createService.js', '/docker.sock'],
  command='-ttl 30 -environment dev -s /docker.sock -domain docker -name \
           skydns_{uid} -plugins /createService.js'.format(uid=uid))

client.start(
  container=skydock,
  binds={ args.create_service: { 'bind': '/createService.js', 'ro': True },
          '/var/run/docker.sock': { 'bind': '/docker.sock', 'ro': False } })

skydns_config = client.inspect_container(skydns)
dns = skydns_config['NetworkSettings']['IPAddress']

output = collections.defaultdict(list)
output['op_dns'] = dns

for cfg in configs:
  node_type = cfg['nodes']['node']['sys.config']['node_type']
  node_name = cfg['nodes']['node']['vm.args']['name']
  output['op_{type}_nodes'.format(type=node_type)].append(node_name)

  (name, sep, hostname) = node_name.partition('@')

  command = \
  '''set -e
cat <<"EOF" > /tmp/gen_dev_args.json
{gen_dev_args}
EOF
escript gen_dev.erl /tmp/gen_dev_args.json
/root/bin/node/bin/oneprovider_node console'''
  command = command.format(gen_dev_args=json.dumps(cfg))

  container = client.create_container(
    image=args.image,
    hostname=hostname,
    detach=True,
    stdin_open=True,
    tty=True,
    working_dir='/root/build',
    name='{name}_{uid}'.format(name=name, uid=uid),
    volumes=['/root/build'],
    command=['bash', '-c', command])

  client.start(
    container=container,
    binds={ args.bin:  { 'bind': '/root/build', 'ro': True } },
    dns=[dns])

print(json.dumps(output))
