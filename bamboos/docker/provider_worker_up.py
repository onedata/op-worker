#!/usr/bin/env python

<<<<<<< HEAD
"""A script to brings up a set of oneprovider nodes. They can create separate
=======
"""Author: Konrad Zemek
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

A script to brings up a set of oneprovider nodes. They can create separate
>>>>>>> 5471c449bf400533a81e05e495b9bb70cfd66efd
clusters.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function
import json

from environment import common, provider


parser = common.standard_arg_parser('Bring up oneprovider worker nodes.')
parser.add_argument(
    '-l', '--logdir',
    action='store',
    default=None,
    help='path to a directory where the logs will be stored',
    dest='logdir')

args = parser.parse_args()
output = provider.up(args.image, args.bin, args.logdir, args.dns, args.uid,
                     args.config_path)

print(json.dumps(output))
