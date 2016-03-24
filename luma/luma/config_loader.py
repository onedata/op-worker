from flask import json
from app import db
from model import StorageIdToTypeMapping, GeneratorsMapping, \
    UserCredentialsMapping


def load_storage_id_to_type_mapping(app, storage_id_to_type_file):
    """Loads storage id to type mapping from file into database"""
    with app.app_context():
        with open(storage_id_to_type_file) as data_file:
            data = json.load(data_file)

            if not isinstance(data, list):
                raise RuntimeError(
                    'Invalid file format, should contain list of id to type mapping')

            for entry in data:
                mapping = StorageIdToTypeMapping(entry['storage_id'],
                                                 entry['storage_type'])
                db.session.merge(mapping)
            db.session.commit()


def load_generators_mapping(app, plugins, generators_file):
    """Loads generators mapping from file into database"""
    with app.app_context():
        with open(generators_file) as data_file:
            data = json.load(data_file)
            if not isinstance(data, list):
                raise RuntimeError(
                    'Invalid file format, should contain list of generators mapping')

            for entry in data:
                if 'storage_id' in entry:
                    storage_id = entry['storage_id']
                elif 'storage_type' in entry:
                    storage_id = entry['storage_type']
                else:
                    raise RuntimeError(
                        'Generators mapping must contain storage_id or storage_type')

                if entry['generator_id'] not in plugins.get_available_plugins():
                    raise RuntimeError('Generator {} does not exists'.format(
                        entry['generator_id']))
                mapping = GeneratorsMapping(storage_id, entry['generator_id'])
                db.session.merge(mapping)
            db.session.commit()


def load_user_credentials_mapping(app, user_credentials_file):
    """Loads user credentials mapping from file into database"""
    with app.app_context():
        with open(user_credentials_file) as data_file:
            data = json.load(data_file)

            if not isinstance(data, list):
                raise RuntimeError(
                    'Invalid file format, should contain list of credentials')

            for entry in data:
                mapping = UserCredentialsMapping(entry['global_id'],
                                                 entry['storage_id'],
                                                 json.dumps(
                                                     entry['credentials']))
                db.session.merge(mapping)
            db.session.commit()
