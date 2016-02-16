from app import db


class UserCredentialsMapping(db.Model):
    global_id = db.Column(db.String, primary_key=True)
    storage_id = db.Column(db.String, primary_key=True)
    credentials = db.Column(db.String)

    def __init__(self, global_id, storage_id, credentials):
        self.global_id = global_id
        self.storage_id = storage_id
        self.credentials = credentials

    def __repr__(self):
        return '<UserCredentialsMapping {} {} {}>'.format(self.global_id, self.storage_id, self.credentials)


class GeneratorsMapping(db.Model):
    storage_id = db.Column(db.String, primary_key=True)
    generator_id = db.Column(db.String)

    def __init__(self, storage_id, generator_id):
        self.storage_id = storage_id
        self.generator_id = generator_id

    def __repr__(self):
        return '<GeneratorsMapping {} {}>'.format(self.storage_id, self.generator_id)


class StorageIdToTypeMapping(db.Model):
    storage_id = db.Column(db.String, primary_key=True)
    storage_type = db.Column(db.String, primary_key=True)

    def __init__(self, storage_id, storage_type):
        self.storage_id = storage_id
        self.storage_type = storage_type

    def __repr__(self):
        return '<StorageIdToTypeMapping {} {}>'.format(self.storage_id, self.storage_type)
