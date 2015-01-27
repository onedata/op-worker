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
  help='the path to globalregistry repository (precompiled)',
  dest='bin')

args = parser.parse_args()
client = docker.Client()
cookie = str(int(time.time()))

pull_image(args.image)
pull_image('klaemo/couchdb')

couchdb = client.create_container(
  image='klaemo/couchdb',
  detach=True,
  name='couchdb_{cookie}'.format(cookie=cookie),
  environment={ 'ERL_FLAGS': '-name db@couchdb.{cookie}.dev.docker -setcookie {cookie}'.format(cookie=cookie) },
  hostname='couchdb.{cookie}.dev.docker'.format(cookie=cookie))
client.start(container=couchdb)

config = '''
{{node, "globalregistry@globalregistry.{cookie}.dev.docker"}}.
{{cookie, "{cookie}"}}.
{{db_nodes, "['db@couchdb.{cookie}.dev.docker']"}}.
{{grpcert_domain, "\\"onedata.org\\""}}.
'''.format(cookie=cookie)

command = '''
rsync -rogl /root/bin/ /root/run
cd /root/run
cp -f /vars.config rel/vars.config
rm -Rf rel/globalregistry
cat rel/vars.config
./rebar generate
rel/globalregistry/bin/globalregistry console
'''

with tempfile.NamedTemporaryFile() as config_file:
  config_file.write(config.encode('utf-8'))
  config_file.flush()

  with tempfile.NamedTemporaryFile() as command_file:
    command_file.write(command.encode('utf-8'))
    command_file.flush()

    gr = client.create_container(
      image=args.image,
      hostname='globalregistry.{cookie}.dev.docker'.format(cookie=cookie),
      detach=True,
      stdin_open=True,
      tty=True,
      name='globalregistry_{cookie}'.format(cookie=cookie),
      volumes=['/root/bin', '/vars.config', '/command.sh'],
      command=['sh', '/command.sh'])

    client.start(
      container=gr,
      binds={
        args.bin: { 'bind': '/root/bin', 'ro': True },
        config_file.name: { 'bind': '/vars.config', 'ro': True },
        command_file.name: { 'bind': '/command.sh', 'ro': True }
      },
      links={ 'couchdb_{cookie}'.format(cookie=cookie): 'couchdb.{cookie}.dev.docker'.format(cookie=cookie) })

