#!/usr/bin/env python

import sys
import os
import uuid
import re
import time
from docker import Client

CONFIG = re.sub(r'\s', '',
   '''[
        [
          {name, "%(name)s"},
          {type, %(typ)s},
          {ccm_nodes, %(ccm_nodes)s},
          {cookie, "%(cookie)s"},
          {db_nodes, []},
          {target_dir, "/root"}
        ]
      ].''').replace('"', r'\"')

COMMAND = re.sub(r'\s+', ' ',
  '''cp -RTf /root/{bin,run} &&
     cd /root/run &&
     echo "%(config)s" > gen_dev.args &&
     escript gen_dev.erl &&
     /root/%(typ)s/bin/oneprovider_node foreground''')

def hostname(typ, num):
  return '{typ}{num}.test'.format(typ=typ, num=num)

def node(typ, num):
  hname = hostname(typ, num)
  return '{typ}@{hostname}'.format(typ=typ, hostname=hname)

def command(typ, num, ccms, cookie):
  cfg = CONFIG % { 'name': node(typ, i), 'typ': typ, 'ccm_nodes': ccms, 'cookie': cookie }
  cmd = COMMAND % { 'config': cfg, 'typ': typ }
  return ['bash', '-c', cmd]

def container_name(typ, num, cookie):
  return '{typ}{num}_{cookie}'.format(typ=typ, num=num, cookie=cookie)

def create_container(client, image, typ, num, ccms, cookie):
  return client.create_container(image=image,
                                 command=command(typ, num, ccms, cookie),
                                 hostname=hostname(typ, num),
                                 detach=True,
                                 name=container_name(typ, num, cookie),
                                 volumes='/root/bin')

def ccm_link(cookie):
  return { container_name('ccm', 0, cookie): hostname('ccm', 0) }

def start_container(container, links):
  return client.start(container=container.get('Id'),
                      links=links,
                      binds={ bindir: { 'bind': '/root/bin', 'ro': True } })


client = Client()
image = sys.argv[1]
bindir = sys.argv[2]
ccms_no = int(sys.argv[3])
workers_no = int(sys.argv[4])

cookie = str(int(time.time()))
ccms = str([node('ccm', i) for i in xrange(ccms_no)])
links = {container_name('ccm', i, cookie): hostname('ccm', i) for i in xrange(ccms_no)}

for i in xrange(ccms_no):
  container = create_container(client, image, 'ccm', i, ccms, cookie)
  start_container(container, ccm_link(cookie) if i != 0 else {})

for i in xrange(workers_no):
  container = create_container(client, image, 'worker', i, ccms, cookie)
  start_container(container, links)
