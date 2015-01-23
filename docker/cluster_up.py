#!/usr/bin/env python

import argparse
import sys
import os
import uuid
import re
import time
import docker

CONFIG = re.sub(r'\s', '',
   '''[
        [
          {name, "%(name)s"},
          {type, %(typ)s},
          {ccm_nodes, %(ccm_nodes)s},
          {cookie, "%(cookie)s"},
          {db_nodes, []},
          {workers_to_trigger_init, %(workers)i},
          {target_dir, "/root/bin"}
        ]
      ].''').replace('"', r'\"')

COMMAND = re.sub(r'\s+', ' ',
  '''echo "%(config)s" > /tmp/gen_dev.args &&
     cd /root/build &&
     escript gen_dev.erl /tmp/gen_dev.args &&
     /root/bin/%(typ)s/bin/oneprovider_node foreground''')

def hostname(typ, num, cookie):
  return '{typ}{num}.{cookie}.dev.docker'.format(typ=typ, num=num, cookie=cookie)

def node(typ, num, cookie):
  hname = hostname(typ, num, cookie)
  return '{typ}@{hostname}'.format(typ=typ, hostname=hname)

def command(typ, num, workers_no, ccms, cookie):
  cfg = CONFIG % { 'name': node(typ, i, cookie), 'typ': typ,
                   'ccm_nodes': ccms, 'cookie': cookie, 'workers': workers_no }
  cmd = COMMAND % { 'config': cfg, 'typ': typ }
  return ['bash', '-c', cmd]

def container_name(typ, num, cookie):
  return '{typ}{num}_{cookie}'.format(typ=typ, num=num, cookie=cookie)

def create_container(client, image, typ, num, workers_no, ccms, cookie):
  return client.create_container(image=image,
                                 command=command(typ, num, workers_no, ccms, cookie),
                                 hostname=hostname(typ, num, cookie),
                                 detach=True,
                                 name=container_name(typ, num, cookie),
                                 volumes='/root/build')

def ccm_link(cookie):
  return { container_name('ccm', 0, cookie): hostname('ccm', 0) }

def start_container(container, bindir, dns):
  return client.start(container=container,
                      binds={ bindir: { 'bind': '/root/build', 'ro': True } },
                      dns=[dns])

parser = argparse.ArgumentParser(description='Bring up onedata cluster.')
parser.add_argument('--image', '-i', action='store', default='onedata/worker',
                    help='the image to use for the container', dest='image')
parser.add_argument('--bin', '-b', action='store', default=os.getcwd(),
                    help='the path to onedata repository (precompiled)', dest='bin')
parser.add_argument('--ccms', '-c', action='store', type=int, default=1,
                    help='the number of ccm nodes to spin up', dest='ccms')
parser.add_argument('--workers', '-w', action='store', type=int, default=1,
                    help='the number of worker nodes to spin up', dest='workers')
args = parser.parse_args()


client = docker.Client()
cookie = str(int(time.time()))
ccms = str([node('ccm', i, cookie) for i in xrange(args.ccms)])

skydns = client.create_container(image='crosbymichael/skydns',
                                 detach=True,
                                 name='skydns_'+cookie,
                                 command='-nameserver 8.8.8.8:53 -domain docker')
client.start(container=skydns)

createServicePath = os.path.dirname(os.path.realpath(__file__)) + '/createService.js'

skydock = client.create_container(image='crosbymichael/skydock',
                                  detach=True,
                                  name='skydock_'+cookie,
                                  volumes=['/myplugins.js', '/docker.sock'],
                                  command='-ttl 30 -environment dev -s /docker.sock -domain docker -name skydns_'+cookie+' -plugins /myplugins.js')

client.start(container=skydock,
             binds={ createServicePath: { 'bind': '/myplugins.js', 'ro': True },
                     '/var/run/docker.sock': { 'bind': '/docker.sock', 'ro': False } })

config = client.inspect_container(skydns)
dns = config['NetworkSettings']['IPAddress']

for i in xrange(args.ccms):
  container = create_container(client, args.image, 'ccm', i, args.workers, ccms, cookie)
  start_container(container, args.bin, dns)

for i in xrange(args.workers):
  container = create_container(client, args.image, 'worker', i,  args.workers, ccms, cookie)
  start_container(container, args.bin, dns)
