import hashlib
import ConfigParser
import os

config = ConfigParser.RawConfigParser()
config.read(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), 'generators.cfg'))

LowestUID = config.getint('posix', 'lowest_uid')
HighestUID = config.getint('posix', 'highest_uid')


def gen_storage_id(id):
    m = hashlib.md5()
    m.update(id)
    return LowestUID + int(m.hexdigest(), 16) % HighestUID


def create_user_credentials(global_id, storage_type, storage_id, source_ips,
                            source_hostname, user_details):
    """Creates user credentials for POSIX storage based on provided user data.
    Sample output:
    {
        "uid": 31415
    }
    """
    if global_id == "0":
        return {'uid': 0}

    return {'uid': gen_storage_id(global_id)}
