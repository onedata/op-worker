import __main__
import json
import os
import subprocess

def run(image, docker_host=None, detach=False, dns=[], hostname=None,
        interactive=False, link={}, tty=False, rm=False, reflect=[],
        volumes=[], name=None, workdir=None, run_params=[], command=None):

    cmd = ['docker']

    if docker_host:
        cmd.extend(['-H', docker_host])

    cmd.append('run')

    if detach:
        cmd.append('-d')

    for addr in dns:
        cmd.extend(['--dns', addr])

    if hostname:
        cmd.extend(['-h', hostname])

    if detach or not hasattr(__main__, '__file__'):
        if interactive:
            cmd.append('-i')
        if tty:
            cmd.append('-t')

    for container, alias in link.iteritems():
        cmd.extend(['--link', '{0}:{1}'.format(container, alias)])

    if name:
        cmd.extend(['--name', name])

    if rm:
        cmd.append('--rm')

    for path in reflect:
        vol = '{0}:{0}:rw'.format(os.path.abspath(path))
        cmd.extend(['-v', vol])

    for path, bind, readable in volumes:
        vol = '{0}:{1}:{2}'.format(os.path.abspath(path), bind, readable)
        cmd.extend(['-v', vol])

    if workdir:
        cmd.extend(['-w', workdir])

    cmd.extend(run_params)
    cmd.append(image)

    if isinstance(command, str):
        cmd.extend(['sh', '-c', command])
    elif isinstance(command, list):
        cmd.extend(command)

    if detach:
        return subprocess.check_output(cmd).strip()
    else:
        subprocess.check_call(cmd)


def inspect(container, docker_host=None):
    cmd = ['docker']
    
    if docker_host:
        cmd.extend(['-H', docker_host])

    cmd.extend(['inspect', container])
    out = subprocess.check_output(cmd, universal_newlines=True)
    return json.loads(out)[0]


def remove(containers, docker_host=None, force=False,
           link=False, volumes=False):
    cmd = ['docker']

    if docker_host:
        cmd.extend(['-H', docker_host])

    cmd.append('rm')

    if force:
        cmd.append('-f')

    if link:
        cmd.append('-l')

    if volumes:
        cmd.append('-v')

    cmd.extend(containers)
    subprocess.check_call(cmd)
