#!/usr/bin/env python

"""Author: Michal Stanisz
Copyright (C) 2020 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Runs oneprovider integration tests, providing Erlang's ct_run with every
environmental argument it needs for successful run. The output is put into
'test_distributed/logs'. The (init|end)_per_suite "testcases" are removed from
the surefire.xml output.
This script is similar to ct_run.py but starts environment using onenv and k8s.

All paths used are relative to script's path, not to the running user's CWD.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function

from os.path import expanduser
import argparse
import json
import os
import platform
import re
import shutil
import sys
import time
import glob
import fnmatch
import xml.etree.ElementTree as ElementTree

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(script_dir, 'bamboos/docker'))
from environment import docker, dockers_config
from environment.common import HOST_STORAGE_PATH, remove_dockers_and_volumes

CONFIG_DIRS = ['.docker', '.kube', '.minikube', '.one-env']

def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Run Common Tests.')

    parser.add_argument(
        '--image', '-i',
        action='store',
        default=None,
        help='override of docker image to use as test master.',
        dest='image')

    parser.add_argument(
        '--suite', '-s',
        action='append',
        help='name of the test suite',
        dest='suites')

    parser.add_argument(
        '--case', '-c',
        action='append',
        help='name of the test case',
        dest='cases')

    parser.add_argument(
        '--path-to-sources',
        default=os.path.normpath(os.path.join(os.getcwd(), '..')),
        help='path ot sources to be mounted in onenv container. Use when sources are outside HOME directory',
        dest='path_to_sources')

    args = parser.parse_args()
    dockers_config.ensure_image(args, 'image', 'worker')

    excl_mods = glob.glob(
        os.path.join(script_dir, 'test_distributed', '*.erl'))
    excl_mods = [os.path.basename(item)[:-4] for item in excl_mods]


    command = prepare_docker_command(args)
    remove_dockers_and_volumes()
    ret = start_test_docker(command, args)
    remove_onenv_container()

    if ret != 0 and not skipped_test_exists(os.path.join(script_dir, "test_distributed/logs/*/surefire.xml")):
        ret = 0

    sys.exit(ret)
    
    
def prepare_ct_command(args):
    ct_command = ['ct_run',
                  '-abort_if_missing_suites',
                  '-dir', '.',
                  '-logdir', './logs/',
                  '-ct_hooks', 'cth_surefire', '[{path, "surefire.xml"}]',
                  'and', 'cth_logger', 
                  'and', 'cth_onenv_up', 
                  'and', 'cth_mock',
                  'and', 'cth_posthook',
                  '-noshell',
                  '-name', 'testmaster@testmaster',
                  '-hidden',
                  '-include', '../include', '../_build/default/lib']

    code_paths = ['-pa']
    
    code_paths.extend(
        glob.glob(os.path.join(script_dir, '_build/default/lib', '*', 'ebin')))
    ct_command.extend(code_paths)

    ct_command.extend(['-env', 'path_to_sources', os.path.normpath(os.path.join(os.getcwd(), args.path_to_sources))])

    if args.suites:
        ct_command.append('-suite')
        ct_command.extend(args.suites)

    if args.cases:
        ct_command.append('-case')
        ct_command.extend(args.cases)
        
    return ct_command
    
    
def prepare_docker_command(args):
    ct_command = prepare_ct_command(args)
    
    command = '''
import os, shutil, subprocess, sys, stat

home = '{user_home}'
os.environ['HOME'] = home
if not os.path.exists(home):
    os.makedirs(home)

if {shed_privileges}:
    os.environ['PATH'] = os.environ['PATH'].replace('sbin', 'bin')
    os.chown(home, {uid}, {gid})
    docker_gid = os.stat('/var/run/docker.sock').st_gid
    os.chmod('/etc/hosts', 0o666)
    os.chmod('/etc/resolv.conf', 0o666)
    os.setgroups([docker_gid])
    os.setregid({gid}, {gid})
    os.setreuid({uid}, {uid})

config_dirs={config_dirs}

# Try to copy config dirs, continue if it fails (might not exist on host).
for dirname in config_dirs:
    try:
        shutil.copytree(os.path.join('/tmp', dirname), os.path.join(home, dirname))
    except:
        pass

command = {cmd}
ret = subprocess.call(command)

import xml.etree.ElementTree as ElementTree, glob, re
for file in glob.glob('logs/*/surefire.xml'):
    tree = ElementTree.parse(file)
    for suite in tree.findall('.//testsuite'):
        for test in suite.findall('testcase'):
            match = re.match('(init|end)_per_suite', test.attrib['name'])
            if match is not None:
                suite.remove(test)
    tree.write(file)

sys.exit(ret)
    '''
    return command.format(
        uid=os.geteuid(),
        gid=os.getegid(),
        cmd=ct_command,
        user_home=expanduser('~'),
        config_dirs=CONFIG_DIRS,
        shed_privileges=(platform.system() == 'Linux'))

    
def start_test_docker(command, args):
    volumes = []
    for dirname in CONFIG_DIRS:
        path = expanduser(os.path.join('~', dirname))
        if os.path.isdir(path):
            volumes += [(path, os.path.join('/tmp', dirname), 'ro')]
            
    return docker.run(tty=True,
                      rm=True,
                      interactive=True,
                      workdir=os.path.join(script_dir, 'test_distributed'),
                      volumes=volumes,
                      reflect=[
                          (args.path_to_sources, 'rw'),
                          (script_dir, 'rw'),
                          ('/var/run/docker.sock', 'rw'),
                          (HOST_STORAGE_PATH, 'rw'),
                          ('/etc/passwd', 'ro')
                      ],
                      name='testmaster',
                      hostname='testmaster.test',
                      image=args.image,
                      command=['python', '-c', command]) 


def skipped_test_exists(junit_report_path):
    reports = glob.glob(junit_report_path)
    # if there are many reports, check only the last one
    reports.sort()
    tree = ElementTree.parse(reports[-1])
    testsuites = tree.getroot()
    for testsuite in testsuites:
        if testsuite.attrib['skipped'] != '0':
            return True
    return False
    
    
def remove_onenv_container():
    container = docker.ps(all=True, quiet=True, filters=[('name', 'one-env')])
    if container:
        docker.remove(container, force=True)
    
    
if __name__ == '__main__': 
    main()
