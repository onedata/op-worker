# coding=utf-8
"""Author: Krzysztof Trzepla
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up a set of onepanel nodes. They can create separate clusters.
"""

import copy
import json
import os

from . import common, docker, dns, gui


def panel_domain(instance, uid):
    """Formats domain for a docker hosting onepanel."""
    return common.format_hostname(instance, uid)


def panel_hostname(node_name, instance, uid):
    """Formats hostname for a docker hosting onepanel.
    NOTE: Hostnames are also used as docker names!
    """
    return common.format_hostname([node_name, instance], uid)


def panel_erl_node_name(node_name, instance, uid):
    """Formats erlang node name for a vm on onepanel docker.
    """
    hostname = panel_hostname(node_name, instance, uid)
    return common.format_erl_node_name('onepanel', hostname)


def _tweak_config(config, name, onepanel_instance, uid):
    cfg = copy.deepcopy(config)
    cfg['nodes'] = {'node': cfg['nodes'][name]}

    sys_config = cfg['nodes']['node']['sys.config']['onepanel']
    sys_config['generate_test_web_cert'] = True
    sys_config['treat_test_ca_as_trusted'] = True
    sys_config['test_web_cert_domain'] = panel_domain(onepanel_instance, uid)

    vm_args = cfg['nodes']['node']['vm.args']
    vm_args['name'] = panel_erl_node_name(name, onepanel_instance, uid)

    return cfg


def _node_up(image, bindir, config, dns_servers, extra_volumes, logdir):
    node_name = config['nodes']['node']['vm.args']['name']

    (name, sep, hostname) = node_name.partition('@')

    command = \
        '''set -e
mkdir -p /root/bin/node/log/
echo 'while ((1)); do chown -R {uid}:{gid} /root/bin/node/log; sleep 1; done' > /root/bin/chown_logs.sh
bash /root/bin/chown_logs.sh &
cat <<"EOF" > /tmp/gen_dev_args.json
{gen_dev_args}
EOF
escript bamboos/gen_dev/gen_dev.escript /tmp/gen_dev_args.json
/root/bin/node/bin/onepanel console'''
    command = command.format(
        uid=os.geteuid(),
        gid=os.getegid(),
        gen_dev_args=json.dumps({'onepanel': config}))

    bindir = os.path.abspath(bindir)
    volumes = [(bindir, bindir, 'ro')]
    volumes.extend(extra_volumes)

    if logdir:
        logdir = os.path.join(os.path.abspath(logdir), hostname)
        os.makedirs(logdir)
        volumes.append((logdir, '/root/bin/node/log', 'rw'))

    container = docker.run(
        image=image,
        name=hostname,
        hostname=hostname,
        detach=True,
        interactive=True,
        tty=True,
        workdir=bindir,
        volumes=volumes,
        dns_list=dns_servers,
        privileged=True,
        command=command)

    return (
        {
            'docker_ids': [container],
            'onepanel_nodes': [node_name]
        }
    )


def _configure_posix_storage(storages):
    posix_storages = filter(lambda storage: storage.get('type') == 'posix',
                            storages)
    posix_storage_out = {}
    volumes = []
    for storage in posix_storages:
        name = storage.get('name')
        if name:
            (host_path, docker_path, mode) = common.volume_for_storage(name)
            volumes.append((host_path, docker_path, mode))
            posix_storage_out[name] = {
                "host_path": host_path, "docker_path": docker_path
            }

    return volumes, {"storages": {"posix": posix_storage_out}}


def up(image, bindir, dns_server, uid, config_path, storages_dockers=None,
       logdir=None):
    config = common.parse_json_config_file(config_path)
    input_dir = config['dirs_config']['onepanel']['input_dir']
    dns_servers, output = dns.maybe_start(dns_server, uid)

    for onepanel_instance in config['onepanel_domains']:
        instance_config = config['onepanel_domains'][onepanel_instance]

        image = instance_config.get('image', image)
        os_config_name = instance_config.get('os_config')
        storages = config.get('os_configs', {}).get(os_config_name, {}). \
            get('storages', [])

        extra_volumes, posix_storage_out = _configure_posix_storage(storages)
        common.merge(output, posix_storage_out)

        # Check if gui override is enabled in env and start it
        if 'gui_override' in instance_config and isinstance(
                instance_config['gui_override'], dict):
            gui_config = instance_config['gui_override']
            onepanel_hostname = panel_domain(onepanel_instance, uid)
            extra_volumes.extend(
                gui.extra_volumes(gui_config, onepanel_hostname))
            gui.override_gui(gui_config, onepanel_hostname)

        gen_dev_cfg = {
            'config': {
                'input_dir': input_dir,
                'target_dir': '/root/bin'
            },
            'nodes': instance_config['onepanel']
        }

        configs = [_tweak_config(gen_dev_cfg, node, onepanel_instance, uid)
                   for node in gen_dev_cfg['nodes']]

        for cfg in configs:
            node_out = _node_up(image, bindir, cfg, dns_servers, extra_volumes,
                                logdir)
            common.merge(output, node_out)

            # Check if gui livereload is enabled in env and turn it on
            if 'gui_override' in instance_config and isinstance(
                    instance_config['gui_override'], dict):
                gui_config = instance_config['gui_override']
                livereload_flag = gui_config['livereload']
                if livereload_flag:
                    livereload_dir = gui_config['mount_path']
                    gui.run_livereload(node_out['docker_ids'][0], livereload_dir)

    return output
