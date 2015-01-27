#!/usr/bin/env python

import collections
import argparse
import os
import time
import docker
import json
import copy
import tempfile

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
  'config_path',
  action='store',
  help='path to gen_dev_args.json that will be used to configure the cluster')

args = parser.parse_args()
client = docker.Client()
cookie = str(int(time.time()))

config = parse_config(args.config_path)
config['config']['target_dir'] = '/root/bin'
configs = [tweak_config(config, node) for node in config['nodes']]

pull_image(args.image)
pull_image('crosbymichael/skydns')
pull_image('crosbymichael/skydock')

skydns = client.create_container(
  image='crosbymichael/skydns',
  detach=True,
  name='skydns_{cookie}'.format(cookie=cookie),
  command='-nameserver 8.8.8.8:53 -domain docker')
client.start(container=skydns)

createServicePath = os.path.dirname(os.path.realpath(__file__)) + '/createService.js'

skydock = client.create_container(
  image='crosbymichael/skydock',
  detach=True,
  name='skydock_{cookie}'.format(cookie=cookie),
  volumes=['/createService.js', '/docker.sock'],
  command='-ttl 30 -environment dev -s /docker.sock -domain docker -name \
           skydns_{cookie} -plugins /createService.js'.format(cookie=cookie))

client.start(
  container=skydock,
  binds={ createServicePath: { 'bind': '/createService.js', 'ro': True },
          '/var/run/docker.sock': { 'bind': '/docker.sock', 'ro': False } })

skydns_config = client.inspect_container(skydns)
dns = skydns_config['NetworkSettings']['IPAddress']

output = collections.defaultdict(list)
output['cookie'] = cookie

for cfg in configs:
  node_type = cfg['nodes']['node']['sys.config']['node_type']
  node_name = cfg['nodes']['node']['vm.args']['name']
  output[node_type].append(node_name)

  (name, sep, hostname) = node_name.partition('@')
  temp = tempfile.NamedTemporaryFile()
  temp.write(json.dumps(cfg).encode('utf-8'))

  container = client.create_container(
    image=args.image,
    hostname= hostname,
    detach=True,
    stdin_open=True,
    tty=True,
    working_dir='/root/build',
    name='{name}_{cookie}'.format(name=name, cookie=cookie),
    volumes=['/root/build', '/tmp/gen_dev_args.json'],
    command=['bash', '-c', 'escript gen_dev.erl /tmp/gen_dev_args.json && \
                            /root/bin/node/bin/oneprovider_node console'])

  client.start(
    container=container,
    binds={
      args.bin:  { 'bind': '/root/build', 'ro': True },
      temp.name: { 'bind': '/tmp/gen_dev_args.json', 'ro': True } },
    dns=[dns])

print(json.dumps(output))
