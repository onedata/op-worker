# coding=utf-8
"""Author: Lukasz Opiola
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Utility module to handle livereload of Ember GUI (for development purposes) on
dockers.

This module only handles the reload of GUI page on static files change.
If automatic rebuilding of GUI is required, it should be run independently and
should place compiled files in the static root dir which was given
as dir_to_watch.
"""

import os
from . import common, docker

def run(container_id, dir_to_watch, detach=True):
    """
    Starts a process on given docker that monitors changes in GUI static root
    and forces a page reload using websocket connection to client.
    The WS connection is created when livereload script is injected on the page
    - this must be done from the Ember client app.
    """

    # Copy the js script that start livereload and a cert that will
    # allow to start a https server for livereload, and start livereload.
    command = '''\
mkdir -p /tmp/gui_livereload
cd /tmp/gui_livereload
cat <<"EOF" > /tmp/gui_livereload/cert.pem
{gui_livereload_cert}
EOF
npm link livereload
cat <<"EOF" > gui_livereload.js
{gui_livereload}
EOF
node gui_livereload.js {dir_to_watch} poll /tmp/gui_livereload/cert.pem'''
    js_path = os.path.join(common.get_script_dir(), 'gui_livereload.js')
    cert_path = os.path.join(common.get_script_dir(), 'gui_livereload_cert.pem')
    command = command.format(
        dir_to_watch=dir_to_watch,
        gui_livereload_cert=open(cert_path, 'r').read(),
        gui_livereload=open(js_path, 'r').read())

    return docker.exec_(
        container=container_id,
        detach=detach,
        interactive=True,
        tty=True,
        command=command,
        output=True)
