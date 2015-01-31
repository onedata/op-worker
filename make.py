#!/usr/bin/env python

import __main__
import argparse
import os
import subprocess

from os.path import expanduser

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Run make in the full development environment.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/builder',
    help='the image to use for building',
    dest='image')

parser.add_argument(
    '--src', '-s',
    action='store',
    default=os.getcwd(),
    help='the source directry to run make from',
    dest='src')

parser.add_argument(
    '--dst', '-d',
    action='store',
    default=os.getcwd(),
    help='the directory to store the build in',
    dest='dst')

parser.add_argument(
    '--keys', '-k',
    action='store',
    default=expanduser("~/.ssh"),
    help='the ssh keys directory needed for dependency fetching',
    dest='keys')

parser.add_argument(
    '--reflect-volume', '-r',
    action='append',
    default=[],
    help='path to file which will be directly reflected in docker\'s filesystem',
    dest='reflect')

parser.add_argument(
    'params',
    action='store',
    nargs='*',
    help='parameters that will be passed to `make`')

args = parser.parse_args()

command = \
'''cp --recursive --no-target-directory --force /root/keys /root/.ssh
chown --recursive root:root /root/.ssh
chmod 700 /root/.ssh
chmod 600 /root/.ssh/*
eval $(ssh-agent)
ssh-add
rsync --archive /root/src/ /root/bin
make {params};
chown --recursive --from=root {uid}:{gid} .'''
command = command.format(params=' '.join(args.params), uid=os.getuid(), gid=os.getgid())

additional_run_params = []
if not hasattr(__main__, '__file__'):
    additional_run_params.append('-it')

additional_volumes=[]
for path in args.reflect:
  additional_volumes.append('-v')
  additional_volumes.append('{vol}:{vol}:rw'.format(vol=path))

subprocess.call(['docker', 'run', '--rm'] + additional_run_params + [
                 '-v', '{src}:/root/src:ro'.format(src=args.src),
                 '-v', '{dst}:/root/bin:rw'.format(dst=args.dst),
                 '-v', '{keys}:/root/keys:ro'.format(keys=args.keys)] +
                 additional_volumes + [
                 '-w', '/root/bin',
                 args.image, 'sh', '-c', command])
