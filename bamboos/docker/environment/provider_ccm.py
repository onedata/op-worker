"""Brings up a set of oneprovider ccm nodes. They can create separate clusters."""

from __future__ import print_function

import copy
import json
import os

import common
import docker


try:
    import xml.etree.cElementTree as eTree
except ImportError:
    import xml.etree.ElementTree as eTree

try:  # Python 2
    from urllib2 import urlopen
    from urllib2 import URLError
    from httplib import BadStatusLine
except ImportError:  # Python 3
    from urllib.request import urlopen
    from urllib.error import URLError
    from http.client import BadStatusLine


def _tweak_config(config, name, uid):
    cfg = copy.deepcopy(config)
    cfg['nodes'] = {'node': cfg['nodes'][name]}

    sys_config = cfg['nodes']['node']['sys.config']
    sys_config['ccm_nodes'] = [common.format_hostname(n, uid) for n in
                               sys_config['ccm_nodes']]

    vm_args = cfg['nodes']['node']['vm.args']
    vm_args['name'] = common.format_hostname(vm_args['name'], uid)

    return cfg


def _node_up(image, bindir, logdir, uid, config, dns_servers):
    node_name = config['nodes']['node']['vm.args']['name']

    (name, sep, hostname) = node_name.partition('@')

    command = \
        '''set -e
cat <<"EOF" > /tmp/gen_dev_args.json
{gen_dev_args}
EOF
escript bamboos/gen_dev/gen_dev.escript /tmp/gen_dev_args.json
/root/bin/node/bin/op_ccm console'''
    command = command.format(
        gen_dev_args=json.dumps({'op_ccm': config}),
        uid=os.geteuid(),
        gid=os.getegid())

    logdir = os.path.join(os.path.abspath(logdir), name)
    volumes = [(bindir, '/root/build', 'ro')]
    volumes.extend([(logdir, '/root/bin/node/log', 'rw')] if logdir else [])

    container = docker.run(
        image=image,
        hostname=hostname,
        detach=True,
        interactive=True,
        tty=True,
        workdir='/root/build',
        name=common.format_dockername(name, uid),
        volumes=volumes,
        dns_list=dns_servers,
        command=command)

    return (
        [container],
        {
            'docker_ids': [container],
            'op_ccm_nodes' : [node_name]
        }
    )

def up(image, bindir, logdir, dns, uid, config_path):
    config = common.parse_json_file(config_path)['op_ccm']
    config['config']['target_dir'] = '/root/bin'
    configs = [_tweak_config(config, node, uid) for node in config['nodes']]

    dns_servers, output = common.set_up_dns(dns, uid)
    ccms = []

    for cfg in configs:
        ccm, node_out = _node_up(image, bindir, logdir, uid, cfg,
                                         dns_servers)
        ccms.extend(ccm)
        common.merge(output, node_out)

    if logdir:
        for node in ccms:
            docker.exec_(node, ['chown', '-R',
                                '{0}:{1}'.format(os.geteuid(), os.getegid()),
                                '/root/bin/node/log/'])

    return output
