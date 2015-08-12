#!/usr/bin/env python

"""Author: Tomasz Lichon
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

A script to bring up a set of oneprovider ccm nodes. They can create separate
clusters.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function
import json

from environment import common, provider_ccm


parser = common.standard_arg_parser('Bring up op_ccm nodes.')
parser.add_argument(
    '-l', '--logdir',
    action='store',
    default=None,
    help='path to a directory where the logs will be stored',
    dest='logdir')

args = parser.parse_args()
output = provider_ccm.up(args.image, args.bin, args.logdir, args.dns, args.uid,
                     args.config_path)

print(json.dumps(output))
