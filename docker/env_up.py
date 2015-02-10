#!/usr/bin/env python

"""
Brings up a DNS server with container (skydns + skydock) that allow
different dockers to see each other by hostnames.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function

import argparse
import collections
import docker
import json
import os
import time
import subprocess

def run_command(cmd):
    return subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.PIPE).communicate()[0]

dns_config = json.loads(run_command(
    os.path.dirname(os.path.realpath(__file__)) + '/dns_up.py'))
print(dns_config['dns'])