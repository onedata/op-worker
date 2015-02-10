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

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def run_command(cmd):
    return subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.PIPE).communicate()[0]

uid = str(int(time.time()))
dns_config = json.loads(run_command([get_script_dir() + '/dns_up.py', '--uid', uid]))
print(dns_config['dns'])