#!/usr/bin/env python

import argparse
import docker
import os
import re
import sys
from os.path import expanduser

parser = argparse.ArgumentParser(description='Run make in the full development environment.')
parser.add_argument('--image', '-i', action='store', default='onedata/builder',
                    help='the image to use for building', dest='image')
parser.add_argument('--src', '-s', action='store', default=os.getcwd(),
                    help='the source directry to run make from', dest='src')
parser.add_argument('--dst', '-d', action='store', default=os.getcwd(),
                    help='the directory to store the build in', dest='dst')
parser.add_argument('--keys', '-k', action='store', default=expanduser("~/.ssh"),
                    help='the ssh keys directory needed for dependency fetching', dest='keys')
parser.add_argument('params', action='store', nargs='*',
                    help='parameters that will be passed to `make`')
args = parser.parse_args()

command = re.sub(r'\s+', ' ',
  '''
    cp -RTf /root/keys /root/.ssh &&
    chown -R root:root /root/.ssh &&
    rsync -rog --exclude=.git /root/src /root/bin &&
    cd /root/bin &&
    make %(params)s &&
    find . -user root -exec chown --reference /root/bin/[Mm]akefile -- {} +
  ''' % { 'params': ' '.join(args.params) } )

client = docker.Client()

container = client.create_container(image=args.image,
                                    tty=True,
                                    command=['/bin/bash', '-c', command],
                                    volumes=['/root/src', '/root/bin', '/root/keys'])

client.start(container=container,
             binds={ args.src:  { 'bind': '/root/src', 'ro': True },
                     args.dst:  { 'bind': '/root/bin', 'ro': False },
                     args.keys: { 'bind': '/root/keys', 'ro': True }})

logs = client.attach(container=container, stdout=True, stderr=True, stream=True, logs=True)
for log in logs:
  sys.stdout.write(log.decode('utf-8'))
