"""Author: Michal Zmuda
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up a set of cluster-worker nodes. They can create separate clusters.
"""

import os
from . import docker, common, worker, gui_livereload

DOCKER_BINDIR_PATH = '/root/build'


def up(image, bindir, dns_server, uid, config_path, logdir=None,
       dnsconfig_path=None):
    if dnsconfig_path is None:
        config = common.parse_json_config_file(config_path)
        input_dir = config['dirs_config']['oz_worker']['input_dir']

        dnsconfig_path = os.path.join(os.path.abspath(bindir), input_dir,
                                      'data', 'dns.config')

    return worker.up(image, bindir, dns_server, uid, config_path,
                     OZWorkerConfigurator(dnsconfig_path), logdir)


class OZWorkerConfigurator:
    def __init__(self, dnsconfig_path):
        self.dnsconfig_path = dnsconfig_path

    def tweak_config(self, cfg, uid, instance):
        sys_config = cfg['nodes']['node']['sys.config'][self.app_name()]
        sys_config['external_ip'] = {'string': 'IP_PLACEHOLDER'}

        if 'http_domain' in sys_config:
            domain = worker.cluster_domain(instance, uid)
            sys_config['http_domain'] = {'string': domain}

        return cfg

    # Called BEFORE the instance (cluster of workers) is started
    def pre_configure_instance(self, instance, uid, config):
        this_config = config[self.domains_attribute()][instance]
        print('GUI for: {0}'.format(common.format_hostname(instance, uid)))
        # Prepare static dockers with GUI (if required) - they are reused by
        # whole cluster (instance)
        if 'gui_override' in this_config and isinstance(
                this_config['gui_override'], dict):
            gui_config = this_config['gui_override']
            mount_path = gui_config['mount_path']
            print('    static root: {0}'.format(mount_path))
            if 'host' in gui_config['mount_from']:
                host_volume_path = gui_config['mount_from']['host']
                print('    from host:   {0}'.format(host_volume_path))
            elif 'docker' in gui_config['mount_from']:
                static_docker_image = gui_config['mount_from']['docker']
                # Create volume name from docker image name and instance name
                volume_name = self.gui_files_volume_name(
                    static_docker_image, instance)
                # Create the volume from given image
                docker.create_volume(
                    path=mount_path,
                    name=volume_name,
                    image=static_docker_image,
                    command='/bin/true')
                print('    from docker: {0}'.format(static_docker_image))
            livereload_flag = gui_config['livereload']
            print('    livereload:  {0}'.format(livereload_flag))
        else:
            print('    config not found, skipping')

    # Called AFTER the instance (cluster of workers) has been started
    def post_configure_instance(self, bindir, instance, config,
                                container_ids, output,
                                storages_dockers=None):
        this_config = config[self.domains_attribute()][instance]
        # Check if gui_livereload is enabled in env and turn it on
        if 'gui_override' in config and isinstance(config['gui_override'],
                                                   dict):
            gui_config = this_config['gui_override']
            livereload_flag = gui_config['livereload']
            livereload_dir = gui_config['mount_path']
            if livereload_flag:
                for container_id in container_ids:
                    gui_livereload.run(container_id, livereload_dir)

    def pre_start_commands(self, domain):
        return '''
sed -i.bak s/\"IP_PLACEHOLDER\"/\"`ip addr show eth0 | grep "inet\\b" | awk '{{print $2}}' | cut -d/ -f1`\"/g /tmp/gen_dev_args.json
escript bamboos/gen_dev/gen_dev.escript /tmp/gen_dev_args.json
mkdir -p /root/bin/node/data/
touch /root/bin/node/data/dns.config
sed -i.bak s/onedata.org/{domain}/g /root/bin/node/data/dns.config
        '''.format(domain=domain)

    def extra_volumes(self, config, bindir, instance):
        extra_volumes = []
        # Check if gui mount is enabled in env and add required volumes
        if 'gui_override' in config and isinstance(config['gui_override'], dict):
            gui_config = config['gui_override']
            if 'host' in gui_config['mount_from']:
                # Mount a path on host to static root dir on OZ docker
                mount_path = gui_config['mount_path']
                host_volume_path = gui_config['mount_from']['host']
                extra_volumes.append((host_volume_path, mount_path, 'ro'))
            elif 'docker' in gui_config['mount_from']:
                static_docker_image = gui_config['mount_from']['docker']
                # Create volume name from docker image name
                volume_name = self.gui_files_volume_name(
                    static_docker_image, instance)
                extra_volumes.append({'volumes_from': volume_name})
        return extra_volumes

    def app_name(self):
        return "oz_worker"

    def domains_attribute(self):
        return "zone_domains"

    def domain_env_name(self):
        return "zone_domain"

    def nodes_list_attribute(self):
        return "oz_worker_nodes"

    # Create volume name from docker image name and instance name
    def gui_files_volume_name(self, image_name, instance_name):
        volume_name = image_name.split('/')[-1].replace(
            ':', '_').replace('-', '_')
        return '{0}_{1}'.format(instance_name, volume_name)
