#!/usr/bin/env python

import argparse
import os
import sys
sys.path.insert(0, 'bamboos/docker')
import docker

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Run Common Tests.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/worker',
    help='docker image to use as a test master',
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

args = parser.parse_args()
script_dir = os.path.dirname(os.path.realpath(__file__))

ct_command = ['ct_run',
              '-dir', '.',
              '-logdir', './logs/',
              '-ct_hooks', 'cth_surefire', '[{path, "surefire.xml"}]',
              '-noshell',
              '-name', 'tester']

if args.suites:
    ct_command.append('-suite')
    ct_command.extend(args.suites)

if args.cases:
    ct_command.append('-case')
    ct_command.extend(args.cases)

command = '''
import os, subprocess, sys, stat
docker_gid = os.stat('/var/run/docker.sock').st_gid
os.chmod('/etc/resolv.conf', 0o666)
os.setgroups([docker_gid])
os.setregid({gid}, {gid})
os.setreuid({uid}, {uid})
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
command = command.format(uid=os.geteuid(), gid=os.getegid(), cmd=ct_command)

ret = docker.run(tty=True,
                 rm=True,
                 interactive=True,
                 workdir=os.path.join(script_dir, 'test_distributed'),
                 reflect=[script_dir,
                          '/var/run/docker.sock',
                          '/usr/bin/docker'],
                 hostname='test.master',
                 image=args.image,
                 command=['python', '-c', command])
sys.exit(ret)
