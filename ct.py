#!/usr/bin/env python

from distutils.spawn import find_executable
import __main__
import argparse
import os
import sys
import subprocess

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Run make in the full development environment.')

parser.add_argument(
    '--suite', '-s',
    action='store',
    help='name of the test suite',
    dest='suite')

parser.add_argument(
    '--case', '-c',
    action='store',
    help='name of the test case',
    dest='case')

args = parser.parse_args()
script_dir = os.path.dirname(os.path.realpath(__file__))
docker_sock = '/var/run/docker.sock'

if args.case and not args.suite:
    print('--case can only be used together with --suite')
    sys.exit()

additional_run_params = []
if not hasattr(__main__, '__file__'):
    additional_run_params.append('-it')

additional_args = []
if args.suite:
    additional_args.append(args.suite)
if args.case:
    additional_args.append(args.case)

subprocess.call(['docker', 'run', '--rm'] + additional_run_params + [
                 '-w', script_dir,
                 '-v', '{dir}:{dir}'.format(dir=script_dir),
                 '-v', '{sock}:{sock}'.format(sock=docker_sock),
                 '-v', '/usr/bin/docker:/usr/bin/docker',
                 '-h', 'd1.local',
                 'onedata/worker', 'sh',
                 './test_distributed/start_distributed_test.sh'] + additional_args)
