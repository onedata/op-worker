"""Author: Michal Zmuda
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up a set of oneprovider worker nodes. They can create separate clusters.
"""

import os
import subprocess
import sys
from . import common, docker, worker, gui_livereload

DOCKER_BINDIR_PATH = '/root/build'


def up(image, bindir, dns_server, uid, config_path, logdir=None, storages_dockers=None):
    return worker.up(image, bindir, dns_server, uid, config_path,
                     ProviderWorkerConfigurator(), logdir, storages_dockers)


class ProviderWorkerConfigurator:
    def tweak_config(self, cfg, uid, instance):
        sys_config = cfg['nodes']['node']['sys.config'][self.app_name()]
        if 'oz_domain' in sys_config:
            oz_hostname = worker.cluster_domain(sys_config['oz_domain'], uid)
            sys_config['oz_domain'] = oz_hostname
        # If livereload bases on gui output dir mount, change the location
        # from where static files are served to that dir.
        if 'gui_livereload' in cfg:
            if cfg['gui_livereload'] in ['mount_output', 'mount_output_poll']:
                sys_config['gui_static_files_root'] = {
                    'string': '/root/gui_static'}
        return cfg

    def pre_start_commands(self, domain):
        return 'escript bamboos/gen_dev/gen_dev.escript /tmp/gen_dev_args.json'

        # Called BEFORE the instance (cluster of workers) is started
    def pre_configure_instance(self, instance, uid, config):
        this_config = config[self.domains_attribute()][instance]
        print('GUI for: {0}'.format(common.format_hostname(instance, uid)))
        # Prepare static dockers with GUI (if required) - they are reused by
        # whole cluster (instance)
        if 'gui' in this_config and isinstance(this_config['gui'], dict):
            mount_path = this_config['gui']['mount_path']
            print('    static root: {0}'.format(mount_path))
            if 'host' in this_config['gui']['mount_from']:
                host_volume_path = this_config['gui']['mount_from']['host']
                print('    from host:   {0}'.format(host_volume_path))
            elif 'docker' in this_config['gui']['mount_from']:
                static_docker_image = this_config['gui']['mount_from']['docker']
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
            livereload_flag = this_config['gui']['livereload']
            print('    livereload:  {0}'.format(livereload_flag))
        else:
            print('    config not found, skipping')

    # Called AFTER the instance (cluster of workers) has been started
    def post_configure_instance(self, bindir, instance, config,
                                   container_ids, output, storages_dockers=None):
        this_config = config[self.domains_attribute()][instance]
        # Check if gui_livereload is enabled in env and turn it on
        if 'gui' in config and isinstance(config['gui'], dict):
            livereload_flag = this_config['gui']['livereload']
            livereload_dir = this_config['gui']['mount_path']
            if livereload_flag:
                for container_id in container_ids:
                    gui_livereload.run(container_id, livereload_dir)
        if 'os_config' in this_config:
            os_config = this_config['os_config']
            create_storages(config['os_configs'][os_config]['storages'],
                            output[self.nodes_list_attribute()],
                            this_config[self.app_name()], bindir, storages_dockers)

    def extra_volumes(self, config, bindir, instance):
        if 'os_config' in config and config['os_config']['storages']:
            if isinstance(config['os_config']['storages'][0], basestring):
                posix_storages = config['os_config']['storages']
            else:
                posix_storages = [s['name'] for s in config['os_config']['storages']
                                  if s['type'] == 'posix']
        else:
            posix_storages = []

        extra_volumes = [common.volume_for_storage(s) for s in posix_storages]
        # Check if gui mount is enabled in env and add required volumes
        if 'gui' in config and isinstance(config['gui'], dict):
            if 'host' in config['gui']['mount_from']:
                # Mount a path on host to static root dir on OP docker
                mount_path = config['gui']['mount_path']
                host_volume_path = config['gui']['mount_from']['host']
                extra_volumes.append((host_volume_path, mount_path, 'ro'))
            elif 'docker' in config['gui']['mount_from']:
                static_docker_image = config['gui']['mount_from']['docker']
                # Create volume name from docker image name
                volume_name = self.gui_files_volume_name(
                    static_docker_image, instance)
                extra_volumes.append({'volumes_from': volume_name})
        return extra_volumes

    def app_name(self):
        return "op_worker"

    def domains_attribute(self):
        return "provider_domains"

    def domain_env_name(self):
        return "provider_domain"

    def nodes_list_attribute(self):
        return "op_worker_nodes"

    # Create volume name from docker image name and instance name
    def gui_files_volume_name(self, image_name, instance_name):
        volume_name = image_name.split('/')[-1].replace(
            ':', '_').replace('-', '_')
        return '{0}_{1}'.format(instance_name, volume_name)


def create_storages(storages, op_nodes, op_config, bindir, storages_dockers):
    # copy escript to docker host
    script_names = {'posix': 'create_posix_storage.escript',
                    's3': 'create_s3_storage.escript',
                    'ceph': 'create_ceph_storage.escript'}
    pwd = common.get_script_dir()
    for _, script_name in script_names.iteritems():
        command = ['cp', os.path.join(pwd, script_name),
                   os.path.join(bindir, script_name)]
        subprocess.check_call(command)
    # execute escript on one of the nodes
    # (storage is common fo the whole provider)
    first_node = op_nodes[0]
    container = first_node.split("@")[1]
    worker_name = container.split(".")[0]
    cookie = op_config[worker_name]['vm.args']['setcookie']
    script_paths = dict(map(lambda (k, v): (k, os.path.join(DOCKER_BINDIR_PATH, v)),
                              script_names.iteritems()))
    for storage in storages:
        if isinstance(storage, basestring):
            storage = {'type': 'posix', 'name': storage}
        if storage['type'] == 'posix':
            st_path = storage['name']
            command = ['escript', script_paths['posix'], cookie,
                       first_node, storage['name'], st_path]
            assert 0 is docker.exec_(container, command, tty=True,
                                     stdout=sys.stdout, stderr=sys.stderr)
        elif storage['type'] == 'ceph':
            config = storages_dockers['ceph'][storage['name']]
            pool = storage['pool'].split(':')[0]
            command = ['escript', script_paths['ceph'], cookie,
                       first_node, storage['name'], "ceph",
                       config['host_name'], pool, config['username'], config['key']]
            assert 0 is docker.exec_(container, command, tty=True,
                                     stdout=sys.stdout, stderr=sys.stderr)
        elif storage['type'] == 's3':
            config = storages_dockers['s3'][storage['name']]
            command = ['escript', script_paths['s3'], cookie,
                       first_node, storage['name'], config['host_name'],
                       storage['bucket'], config['access_key'], config['secret_key'],
                       "iam.amazonaws.com"]
            assert 0 is docker.exec_(container, command, tty=True,
                                     stdout=sys.stdout, stderr=sys.stderr)
        else:
            raise RuntimeError('Unknown storage type: {}'.format(storage['type']))
    # clean-up
    for _, script_name in script_names.iteritems():
        command = ['rm', os.path.join(bindir, script_name)]
        subprocess.check_call(command)