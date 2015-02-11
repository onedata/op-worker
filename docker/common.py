"""A custom utils library used across docker scripts."""

import json
import os
import subprocess


# Returns the absolute path to directory containing given file
def get_file_dir(file_path):
    return os.path.dirname(os.path.realpath(file_path))


# Returns the absolute path to directory containing this file
def get_script_dir():
    return get_file_dir(__file__)


# Runs given script that resides in the same directory and returns JSON.
# Parses the JSON and returns a dict.
def run_script_return_dict(script, args):
    cmd = [get_file_dir(__file__) + '/' + script] + args
    result = subprocess.Popen(cmd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              stdin=subprocess.PIPE).communicate()[0]
    return json.loads(result)


# Parses a JSON file and returns a dict.
def parse_json_file(path):
    with open(path, 'r') as f:
        data = f.read()
        return json.loads(data)