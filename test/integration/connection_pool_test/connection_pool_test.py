"""This module tests connection pool."""

__author__ = "Konrad Zemek"
__copyright__ = """(C) 2015 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
from test_common import *
# noinspection PyUnresolvedReferences
from environment import appmock, common, docker
# noinspection PyUnresolvedReferences
import connection_pool


@pytest.fixture
def endpoint(appmock_client):
    return appmock_client.tcp_endpoint(5555)


@pytest.fixture
def cp(endpoint):
    return connection_pool.ConnectionPoolProxy(3, endpoint.ip, endpoint.port)


@pytest.mark.performance(
    parameters=[msg_num_param(10), msg_size_param(100, 'B')],
    configs={
        'multiple_small_messages': {
            'description': 'Sends multiple small messages using connection '
                           'pool.',
            'parameters': [msg_num_param(1000000)]
        },
        'multiple_large_messages': {
            'description': 'Sends multiple large messages using connection '
                           'pool.',
            'parameters': [msg_num_param(10000), msg_size_param(1, 'MB')]
        }
    })
def test_cp_should_send_messages(result, msg_num, msg_size, endpoint, cp):
    """Sends multiple messages using connection pool and checks whether they
    have been received."""

    msg = random_str(msg_size)

    send_time = Duration()
    for _ in xrange(msg_num):
        with measure(send_time):
            cp.send(msg)

    with measure(send_time):
        endpoint.wait_for_specific_messages(msg, msg_num, timeout_sec=600)

    result.set([
        send_time_param(send_time.ms()),
        mbps_param(msg_num, msg_size, send_time.us()),
        msgps_param(msg_num, send_time)
    ])


@pytest.mark.performance(
    parameters=[msg_num_param(10), msg_size_param(100, 'B')],
    configs={
        'multiple_small_messages': {
            'description': 'Receives multiple small messages using '
                           'connection pool.',
            'parameters': [msg_num_param(10000)]
        },
        'multiple_large_messages': {
            'description': 'Receives multiple large messages using '
                           'connection pool.',
            'parameters': [msg_size_param(1, 'MB')]
        }
    })
def test_cp_should_receive_messages(result, msg_num, msg_size, endpoint, cp):
    """Receives multiple messages using connection pool."""

    msgs = [random_str(msg_size) for _ in xrange(msg_num)]

    recv_time = Duration()
    for msg in msgs:
        with measure(recv_time):
            endpoint.send(msg)

    recv = []
    for _ in msgs:
        with measure(recv_time):
            recv.append(cp.popMessage())

    assert len(msgs) == len(recv)
    assert msgs.sort() == recv.sort()

    result.set([
        recv_time_param(recv_time.ms()),
        mbps_param(msg_num, msg_size, recv_time.us()),
        msgps_param(msg_num, recv_time)
    ])
