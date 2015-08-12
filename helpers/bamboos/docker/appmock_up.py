#!/usr/bin/env python
# coding=utf-8

"""Authors: Łukasz Opioła, Konrad Zemek
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

A script that brings up a set of appmock instances.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function
import json

from environment import appmock, common


parser = common.standard_arg_parser('Bring up appmock nodes.')

args = parser.parse_args()
config = appmock.up(args.image, args.bin, args.dns, args.uid, args.config_path)
print(json.dumps(config))
