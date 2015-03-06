#!/usr/bin/env python

"""A script to brings up a set of oneprovider nodes. They can create separate
clusters.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function
import json

from environment import common, provider


parser = common.standard_arg_parser('Bring up oneprovider nodes.')
parser.add_argument(
    '-l', '--logdir',
    action='store',
    default=None,
    help='path to a directory where the logs will be stored',
    dest='logdir')

args = parser.parse_args()
<<<<<<< HEAD
output = provider.up(args.image, args.bin, args.dns, args.uid, args.logdir,
=======
output = provider.up(args.image, args.bin, args.logdir, args.dns, args.uid,
>>>>>>> 0de40ab1c085f50d87cfdca4417e6665a4ccbc82
                     args.config_path)

print(json.dumps(output))
