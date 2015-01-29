#!/usr/bin/env python

from __future__ import print_function
import argparse
import collections
import copy
import docker
import json
import os
import sys
import time

def pull_image(name):
  try:
    client.inspect_image(name)
  except docker.errors.APIError:
    print('Pulling image {name}'.format(name=name), file=sys.stderr)
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
  help='path to oneclient repository (precompiled)',
  dest='bin')

parser.add_argument(
  '--cert', '-c',
  action='store',
  help='path to certificate to use for connection',
  dest='cert')

parser.add_argument(
  '--key', '-k',
  action='store',
  help='path to certificate key to use for connection',
  dest='key')

parser.add_argument(
  '--token', '-t',
  action='store',
  help='authentication code to use for the client',
  dest='token')

parser.add_argument(
  '--gr-host', '-g',
  action='store',
  help='Global Registry hostname',
  dest='gr_host')

parser.add_argument(
  '--provider-host', '-p',
  action='store',
  help='oneprovider hostname',
  dest='provider_host')

args = parser.parse_args()
client = docker.Client()

pull_image(args.image)

envs = {}
volumes = ['/root/bin']
binds = { args.bin: { 'bind': '/root/bin', 'ro': True } }
command = \
'''cp -f /tmp/cert /root/cert
cp -f /tmp/key /root/key
chown -f root:root /root/cert /root/key
/root/bin/release/oneclient -f'''

if args.provider_host:
    envs['PROVIDER_HOSTNAME'] = args.provider_host

if args.gr_host:
    envs['GLOBAL_REGISTRY_URL'] = args.gr_host

if args.cert:
    envs['X509_USER_CERT'] = '/root/cert'
    binds[args.cert] = { 'bind': '/tmp/cert', 'ro': True }
    volumes.append('/tmp/cert')

if args.key:
    envs['X509_USER_KEY'] = '/root/key'
    binds[args.key] = { 'bind': '/tmp/key', 'ro': True }
    volumes.append('/tmp/key')
else:
    envs['X509_USER_KEY'] = '/root/cert'

if args.token:
    command = '''{cmd} -authentication token <<"EOF"
{token}
EOF'''.format(cmd=command,
                                                             token=args.token)

container = client.create_container(
  image=args.image,
  detach=True,
  environment=envs,
  working_dir='/root/bin',
  volumes=volumes,
  command=['sh', '-c', command])

client.start(
  container=container,
  binds=binds)
