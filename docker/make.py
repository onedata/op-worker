#!/usr/bin/env python

import argparse
import docker
import os
import re
import sys
from os.path import expanduser

def pull_image(name):
  try:
    client.inspect_image(name)
  except docker.errors.APIError:
    print('Pulling image {name}'.format(name=name))
    client.pull(name)


parser = argparse.ArgumentParser(
  formatter_class=argparse.ArgumentDefaultsHelpFormatter,
  description='Run make in the full development environment.')

parser.add_argument('--image', '-i',
                    action='store', default='onedata/builder',
                    help='the image to use for building',
                    dest='image')

parser.add_argument('--src', '-s',
                    action='store',
                    default=os.getcwd(),
                    help='the source directry to run make from',
                    dest='src')

parser.add_argument('--dst', '-d',
                    action='store',
                    default=os.getcwd(),
                    help='the directory to store the build in',
                    dest='dst')

parser.add_argument('--keys', '-k',
                    action='store',
                    default=expanduser("~/.ssh"),
                    help='the ssh keys directory needed for dependency fetching',
                    dest='keys')

parser.add_argument('params',
                    action='store',
                    nargs='*',
                    help='parameters that will be passed to `make`')

args = parser.parse_args()

command = 'cp -RTf /root/keys /root/.ssh && \
           chown -R root:root /root/.ssh && \
           rsync -rog --exclude=.git /root/src /root/bin && \
           make {params} && \
           find . -user root -exec chown --reference /root/bin/[Mm]akefile -- {{}} +'\
            .format(params=' '.join(args.params))

client = docker.Client()

pull_image(args.image)

container = client.create_container(
  image=args.image,
  tty=True,
  working_dir='/root/bin',
  command=['/bin/bash', '-c', command],
  volumes=['/root/src', '/root/bin', '/root/keys'])

client.start(
  container=container,
  binds={ args.src:  { 'bind': '/root/src', 'ro': True },
          args.dst:  { 'bind': '/root/bin', 'ro': False },
          args.keys: { 'bind': '/root/keys', 'ro': True }})

logs = client.attach(container=container, stdout=True, stderr=True, stream=True, logs=True)
for log in logs:
  sys.stdout.write(log.decode('utf-8'))
