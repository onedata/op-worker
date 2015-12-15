#!/usr/bin/env python

"""Author: Rafal Slota
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up a CouchBase cluster.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function
import argparse
import json

from environment import couchbase, common


parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Set up a CouchBase cluster.')

parser.add_argument(
    '-i', '--image',
    action='store',
    default='couchbase/server:latest',
    help='docker image to use for the container',
    dest='image')

parser.add_argument(
    '-d', '--dns',
    action='store',
    default='auto',
    help='IP address of DNS or "none" - if no dns should be started or \
         "auto" - if it should be started automatically',
    dest='dns')

parser.add_argument(
    '-u', '--uid',
    action='store',
    default=common.generate_uid(),
    help='uid that will be concatenated to docker names',
    dest='uid')

parser.add_argument(
    '-n', '--nodes',
    type=int,
    action='store',
    default=2,
    help='number of couchbase nodes to bring up',
    dest='nodes')

args = parser.parse_args()

output = couchbase.up(args.image, args.dns, args.uid, args.nodes)
print(json.dumps(output))
