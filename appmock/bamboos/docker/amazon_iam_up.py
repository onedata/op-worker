#!/usr/bin/env python
# coding=utf-8

"""Authors: Michal Wrona
Copyright (C) 2016 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up a Amazon IAM mock.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function
import argparse
import json

from environment import amazon_iam, common, dockers_config

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Bring up Amazon IAM mock.')

parser.add_argument(
    '-i', '--image',
    action='store',
    default=None,
    help='override of docker image for the container',
    dest='image')

parser.add_argument(
    '-u', '--uid',
    action='store',
    default=common.generate_uid(),
    help='uid that will be concatenated to docker names',
    dest='uid')

args = parser.parse_args()
dockers_config.ensure_image(args, 'image', 'worker')

config = amazon_iam.up(args.image, args.uid)

print(json.dumps(config))
