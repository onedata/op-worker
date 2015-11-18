import os
import sys

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, script_dir)
from test_common import *
from performance import *
from environment import appmock, common, docker
from appmock_client import AppmockClient


@pytest.fixture
def parameters():
    return {}


@pytest.fixture(scope='module')
def _appmock_client(request):
    test_dir = os.path.dirname(os.path.realpath(request.module.__file__))

    result = appmock.up(image='onedata/builder', bindir=appmock_dir,
                        dns_server='none', uid=common.generate_uid(),
                        config_path=os.path.join(test_dir, 'env.json'))

    [container] = result['docker_ids']
    appmock_ip = docker.inspect(container)['NetworkSettings']['IPAddress']. \
        encode('ascii')

    def fin():
        docker.remove([container], force=True, volumes=True)

    request.addfinalizer(fin)
    return AppmockClient(appmock_ip)


@pytest.fixture
def appmock_client(request, _appmock_client):
    _appmock_client.reset_rest_history()
    _appmock_client.reset_tcp_history()
    return _appmock_client


def pytest_addoption(parser):
    parser.addoption('--performance', '-P', action='store_true',
                     help='run performance tests')


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'performance(config): mark test to run in '
        'performance tests with config')


def pytest_runtest_call(item):
    perfmarker = item.get_marker('performance')
    if perfmarker is not None:
        perf_enabled = item.config.getoption('--performance')
        perfmarker.kwargs['perf_enabled'] = perf_enabled
        item.obj = performance(*perfmarker.args, **perfmarker.kwargs)(item.obj)


def pytest_runtest_setup(item):
    perfmarker = item.get_marker('performance')

    if item.config.getoption('-P') and perfmarker is None:
        pytest.skip('test not marked for performance tests')
