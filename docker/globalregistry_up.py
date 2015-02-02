#!/usr/bin/env python

from __future__ import print_function
import argparse
import os
import sys
import time
import docker

def pull_image(name):
  try:
    client.inspect_image(name)
  except docker.errors.APIError:
    print('Pulling image {name}'.format(name=name), file=sys.stderr)
    client.pull(name)


parser = argparse.ArgumentParser(
  formatter_class=argparse.ArgumentDefaultsHelpFormatter,
  description='Bring up globalregistry.')

parser.add_argument(
  '--image', '-i',
  action='store',
  default='onedata/worker',
  help='the image to use for the container',
  dest='image')

parser.add_argument(
  '--bin', '-b',
  action='store',
  default=os.getcwd(),
  help='the path to globalregistry repository (precompiled)',
  dest='bin')

args = parser.parse_args()
client = docker.Client()
cookie = str(int(time.time()))

db_name = 'bigcouch'
db_host = '{name}.{cookie}.dev.docker'.format(name=db_name, cookie=cookie)
db_dockername = '{name}_{cookie}'.format(name=db_name, cookie=cookie)
gr_name = 'globalregistry'
gr_host = '{name}.{cookie}.dev.docker'.format(name=gr_name, cookie=cookie)
gr_dockername = '{name}_{cookie}'.format(name=gr_name, cookie=cookie)

pull_image(args.image)
pull_image('onedata/bigcouch')

db_command = \
'''echo '[httpd]' > /opt/bigcouch/etc/local.ini
echo 'bind_address = 0.0.0.0' >> /opt/bigcouch/etc/local.ini
sed -i 's/-name bigcouch/-name {name}@{host}/g' /opt/bigcouch/etc/vm.args
sed -i 's/-setcookie monster/-setcookie {cookie}/g' /opt/bigcouch/etc/vm.args
/opt/bigcouch/bin/bigcouch'''
db_command = db_command.format(name=db_name, host=db_host, cookie=cookie)

gr_config = \
'''{{node, "{grname}@{grhost}"}}.
{{cookie, "{cookie}"}}.
{{db_nodes, "['{dbname}@{dbhost}']"}}.
{{grpcert_domain, "\\"onedata.org\\""}}.'''
gr_config = gr_config.format(dbname=db_name, dbhost=db_host, cookie=cookie,
                             grname=gr_name, grhost=gr_host)

gr_command = \
'''set -e
rsync -rogl /root/bin/ /root/run
cd /root/run
cat <<"EOF" > rel/vars.config
{cfg}
EOF
rm -Rf rel/globalregistry
cat rel/vars.config
./rebar generate
rel/globalregistry/bin/globalregistry console'''
gr_command = gr_command.format(cfg=gr_config)

bigcouch = client.create_container(
  image='onedata/bigcouch',
  detach=True,
  name=db_dockername,
  hostname=db_host,
  command=['sh', '-c', db_command])
client.start(container=bigcouch)

gr = client.create_container(
  image=args.image,
  hostname=gr_host,
  detach=True,
  stdin_open=True,
  tty=True,
  name=gr_dockername,
  volumes=['/root/bin'],
  command=['bash', '-c', gr_command])

client.start(
  container=gr,
  binds={ args.bin: { 'bind': '/root/bin', 'ro': True } },
  links={ db_dockername: db_host })
