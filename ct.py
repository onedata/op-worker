#!/usr/bin/env python

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

additional_args = []
if args.suite:
	additional_args.append(args.suite)
if args.case:
	additional_args.append(args.case)

subprocess.call(['docker', 'run', '--rm', '-it',
	             '-w', script_dir,
	             '-v', '{dir}:{dir}'.format(dir=script_dir),
	             '-v', '{sock}:{sock}'.format(sock=docker_sock),
	             '-h', 'd1.local',
	             'onedata/worker', 'sh',
	             './test_distributed/start_distributed_test.sh'] + additional_args)
