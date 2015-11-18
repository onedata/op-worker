import os
import random
import string
import sys
from Queue import Queue
from contextlib import contextmanager
from threading import Thread

_script_dir = os.path.dirname(os.path.realpath(__file__))

# Define variables for use in tests
project_dir = os.path.dirname(os.path.dirname(_script_dir))
appmock_dir = os.path.join(project_dir, 'appmock')
docker_dir = os.path.join(project_dir, 'bamboos', 'docker')
annotations_dir = os.path.join(project_dir, 'test', 'annotations')

# Append useful modules to the path
sys.path = [appmock_dir, docker_dir, annotations_dir] + sys.path

from performance import Parameter
from proto import messages_pb2


def random_int():
    return random.randint(1, 100)


def random_str(size=random_int()):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for
                   _ in xrange(size))


def msg_num_param(num):
    return Parameter('msg_num', 'Number of sent messages.', num)


def msg_size_param(size, unit):
    return Parameter('msg_size', 'Size of each message.', size, unit)


def send_time_param(value, unit='ms'):
    return Parameter('send_time', 'Summary time since first messages sent '
                                  'till last message received.', value, unit)


def recv_time_param(value, unit='ms'):
    return Parameter('recv_time', 'Summary time since first reply sent '
                                  'till last reply received.', value, unit)


def communicate_time_param(value, unit='ms'):
    return Parameter('communicate_time', 'Summary time since first message '
                                         'sent till last reply received.',
                     value, unit)


def mbps_param(msg_num, msg_size, us):
    return Parameter('mbps', 'Transfer speed.',
                     (1000000. * msg_num * msg_size) / (1048576 * us), 'MB/s')


def translate_unit(unit):
    if unit == 'kB':
        return 1024
    elif unit == 'MB':
        return 1048576
    else:
        return 1


def _with_reply_process(endpoint, responses, queue):
    for response in responses:
        [received_msg] = endpoint.wait_for_any_messages(return_history=True)
        endpoint.client.reset_tcp_history()

        client_message = messages_pb2.ClientMessage()
        client_message.ParseFromString(received_msg)

        response.message_id = client_message.message_id.encode('utf-8')
        endpoint.send(response.SerializeToString())

        queue.put(client_message)


@contextmanager
def reply(endpoint, responses):
    if not isinstance(responses, list):
        responses = [responses]

    queue = Queue()
    p = Thread(target=_with_reply_process, args=(endpoint, responses, queue))
    p.start()

    try:
        yield queue
    finally:
        p.join()
