"""A custom utils library used across docker scripts."""

import inspect
import json
import os
import subprocess
import time


"""Returns the absolute path to directory containing given file"""
def get_file_dir(file_path):
    return os.path.dirname(os.path.realpath(file_path))


"""Returns the absolute path to directory containing the caller script"""
def get_script_dir():
    caller = inspect.stack()[1]
    caller_mod = inspect.getmodule(caller[0])
    return get_file_dir(caller_mod.__file__)


"""Runs given script that resides in the same directory and returns JSON.
Parses the JSON and returns a dict.
"""
def run_script_return_dict(script, args):
    cmd = [get_file_dir(__file__) + '/' + script] + args
    result = subprocess.Popen(cmd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              stdin=subprocess.PIPE).communicate()[0]
    return json.loads(result)


"""Parses a JSON file and returns a dict."""
def parse_json_file(path):
    with open(path, 'r') as f:
        return json.load(f)


"""Formats hostname for a docker based on node name and uid
node_name can be in format 'somename@' or 'somename'.
This is needed so different components are resolvable through DNS.
"""
def format_hostname(node_name, uid):
    (name, _, _) = node_name.partition('@')
    return '{0}@{0}.{1}.dev.docker'.format(name, uid)


"""Formats docker name based on node name and uid
node_name can be in format 'somename@' or 'somename'.
This is needed so different components are resolvable through DNS.
"""
def format_dockername(node_name, uid):
    (name, _, _) = node_name.partition('@')
    return '{0}_{1}'.format(name, uid)


"""Returns a uid (based on current time), that can be used to group dockers in DNS"""
def generate_uid():
    return str(int(time.time()))
