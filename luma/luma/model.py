from app import db


class UserCredentialsMapping(db.Model):
    """Class represents mapping: (global_id, storage_id) -> credentials.
    Storage type may be provided as id when storage id is not available.
    """
    global_id = db.Column(db.String, primary_key=True)
    storage_id = db.Column(db.String, primary_key=True)
    credentials = db.Column(db.String)

    def __init__(self, global_id, storage_id, credentials):
        self.global_id = global_id
        self.storage_id = storage_id
        self.credentials = credentials

    def __repr__(self):
        return '<UserCredentialsMapping {0} {1} {2}>'.format(self.global_id,
                                                             self.storage_id,
                                                             self.credentials)


class GeneratorsMapping(db.Model):
    """Class represents mapping: storage_id -> generator_id. Storage type
    may be provided as id when storage id is not available. Generator id
    is its filename without extension (e.g. 'ceph' for 'ceph.py').
    """
    storage_id = db.Column(db.String, primary_key=True)
    generator_id = db.Column(db.String)

    def __init__(self, storage_id, generator_id):
        self.storage_id = storage_id
        self.generator_id = generator_id

    def __repr__(self):
        return '<GeneratorsMapping {0} {1}>'.format(self.storage_id,
                                                    self.generator_id)


class StorageIdToTypeMapping(db.Model):
    """Class represents mapping: storage_id -> storage_type."""
    storage_id = db.Column(db.String, primary_key=True)
    storage_type = db.Column(db.String, primary_key=True)

    def __init__(self, storage_id, storage_type):
        self.storage_id = storage_id
        self.storage_type = storage_type

    def __repr__(self):
        return '<StorageIdToTypeMapping {0} {1}>'.format(self.storage_id,
                                                         self.storage_type)
