import os
import rados
import json
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), 'generators.cfg'))

POOL_NAME = config.get('ceph', 'pool_name')
USER = config.get('ceph', 'user')
KEY = config.get('ceph', 'key')
MON_HOST = config.get('ceph', 'mon_host')


def create_user_credentials(global_id, storage_type, storage_id, source_ips,
                            source_hostname, user_details):
    """Creates user credentials for CEPH storage based on provided user data.
    Sample output:
    {
        "user_name": "USER",
        "user_key": "KEY"
    }
    """
    if global_id == "0":
        return {"user_name": USER, "user_key": KEY}
    cluster = rados.Rados(conf=dict())
    cluster.conf_set("key", KEY)
    cluster.conf_set("mon host", MON_HOST)
    cluster.connect()
    user_name = "client.{0}".format(global_id)

    status, response, reason = cluster.mon_command(json.dumps(
        {"prefix": "auth get-or-create", "entity": user_name,
         "caps": ["mon", "allow rw", "osd",
                  "allow rw pool={}".format(POOL_NAME)]}), "")
    if status != 0:
        raise RuntimeError(reason)

    return {"user_name": user_name, "user_key": response.split()[-1]}
