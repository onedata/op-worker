#!/usr/bin/env python

"""
Runs 'make' command in a dockerized development environment. The files are
copied from 'source directory' to 'output directory' and then the make is ran.
The copy operation is optimized, so that only new and changed files are copied.
The script uses user's SSH keys in case dependency fetching is needed.

Run the script with -h flag to learn about script's running options.
"""

import argparse
import docker
import os

from os.path import expanduser

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Run make inside a dockerized development environment.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/builder',
    help='docker image to use for building',
    dest='image')

parser.add_argument(
    '--src', '-s',
    action='store',
    default=os.getcwd(),
    help='source directory to run make from',
    dest='src')

parser.add_argument(
    '--dst', '-d',
    action='store',
    default=os.getcwd(),
    help='destination directory where the build will be stored',
    dest='dst')

parser.add_argument(
    '--keys', '-k',
    action='store',
    default=expanduser("~/.ssh"),
    help='directory of ssh keys used for dependency fetching',
    dest='keys')

parser.add_argument(
    '--reflect-volume', '-r',
    action='append',
    default=[],
    help="host's paths that will be directly reflected in container's filesystem",
    dest='reflect')

parser.add_argument(
    'params',
    action='store',
    nargs='*',
    help='parameters passed to `make`')

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
command = command.format(
    params=' '.join(args.params),
    uid=os.getuid(),
    gid=os.getgid())

volumes = [
    (args.src, '/root/src', 'ro'),
    (args.dst, '/root/bin', 'rw'),
    (args.keys, '/root/keys', 'ro')]

docker.run(tty=True, interactive=True, rm=True, reflect=args.reflect,
           volumes=volumes, workdir='/root/bin', image=args.image,
           command=command)
