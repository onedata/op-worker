import ConfigParser
import os

config = ConfigParser.RawConfigParser()
config.read(os.path.dirname(os.path.realpath(__file__)) + '/generators.cfg')

ACCESS_KEY = config.get('s3', 'access_key')
SECRET_KEY = config.get('s3', 'secret_key')


def create_user_credentials(global_id, storage_type, storage_id, source_ips, source_hostname, user_details):
    return {
        'access_key': ACCESS_KEY,
        'secret_key': SECRET_KEY
    }
