# coding=utf-8
"""Authors: Łukasz Opioła, Konrad Zemek
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up dockers with full onedata environment.
"""

import appmock
import client
import common
import globalregistry
import provider


def up(image, bin_am, bin_gr, bin_op, bin_oc, logdir, config_path):
    config = common.parse_json_file(config_path)
    uid = common.generate_uid()

    output = {
        'docker_ids': [],
        'gr_nodes': [],
        'gr_db_nodes': [],
        'op_ccm_nodes': [],
        'op_worker_nodes': [],
        'appmock_nodes': [],
        'client_nodes': []
    }

    # Start DNS
    [dns], dns_output = common.set_up_dns('auto', uid)
    common.merge(output, dns_output)

    # Start appmock instances
    if 'appmock' in config:
        am_output = appmock.up(image, bin_am, dns, uid, config_path)
        common.merge(output, am_output)

    # Start globalregistry instances
    if 'globalregistry' in config:
        gr_output = globalregistry.up(image, bin_gr, logdir, dns,
                                      uid, config_path)
        common.merge(output, gr_output)

    # Start oneprovider_node instances
    if 'oneprovider_node' in config:
        op_output = provider.up(image, bin_op, logdir, dns, uid, config_path)
        common.merge(output, op_output)

    # Start oneclient instances
    if 'oneclient' in config:
        oc_output = client.up(image, bin_oc, dns, uid, config_path)
        common.merge(output, oc_output)

    return output
