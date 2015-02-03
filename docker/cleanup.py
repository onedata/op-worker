#!/usr/bin/env python

import argparse
import docker

parser = argparse.ArgumentParser(
  formatter_class=argparse.ArgumentDefaultsHelpFormatter,
  description='Clean up dockers.')

parser.add_argument(
  'docker_ids',
  action='store',
  nargs='*',
  help='IDs of dockers to be cleaned up')

args = parser.parse_args()

docker.remove(args.docker_ids, force=True)
