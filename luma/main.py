from flask import json, request
from app import app, db, plugins, args
from config_loader import load_user_credentials_mapping, load_generators_mapping, load_storage_id_to_type_mapping
from model import UserCredentialsMapping, GeneratorsMapping, StorageIdToTypeMapping


def error_message(code, message):
    response = json.jsonify(status='error', message=message)
    response.status_code = code
    return response


def missing_param(param_name):
    return error_message(422, 'Missing parameter: {}'.format(param_name))


@app.route("/get_user_credentials", methods=['GET'])
def get_user_credentials():
    global_id = request.values.get('global_id')
    if not global_id:
        return missing_param('global_id')

    storage_id = request.values.get('storage_id')
    storage_type = request.values.get('storage_type')
    if not storage_id and not storage_type:
        return missing_param('storage_id or storage_type')

    source_ips = request.values.get('source_ips')
    if not source_ips:
        return missing_param('source_ips')
    source_ips = json.loads(source_ips)

    source_hostname = request.values.get('source_hostname')
    if not source_hostname:
        return missing_param('source_hostname')

    user_details = request.values.get('user_details')
    if not user_details:
        return missing_param('user_details')
    user_details = json.loads(user_details)

    if storage_id and not storage_type:
        id_to_type = StorageIdToTypeMapping.query.filter_by(storage_id=storage_id).first()
        if id_to_type:
            storage_type = id_to_type.storage_type

    credentials_mapping = UserCredentialsMapping.query.filter_by(global_id=global_id, storage_id=storage_id).first()
    if not credentials_mapping and storage_type:
        credentials_mapping = UserCredentialsMapping.query.filter_by(global_id=global_id,
                                                                     storage_id=storage_type).first()

    if not credentials_mapping:
        generator_mapping = GeneratorsMapping.query.filter_by(storage_id=storage_id).first()
        if not generator_mapping and storage_type:
            generator_mapping = GeneratorsMapping.query.filter_by(storage_id=storage_type).first()
        if not generator_mapping:
            return error_message(422, 'Generator not defined for given storage id/type')
        try:
            generator = plugins.get_plugin(generator_mapping.generator_id)
            credentials = generator.create_user_credentials(global_id, storage_type, storage_id, source_ips,
                                                            source_hostname, user_details)
        except Exception as e:
            return error_message(500, str(e))

        credentials_mapping = UserCredentialsMapping(global_id, storage_id, json.dumps(credentials))
        db.session.add(credentials_mapping)
        db.session.commit()
    else:
        credentials = json.loads(credentials_mapping.credentials)

    return json.jsonify(status='success', data=credentials)


if __name__ == "__main__":
    if args.credentials_mapping_file:
        load_user_credentials_mapping(args.credentials_mapping_file)
    if args.generators_mapping:
        load_generators_mapping(args.generators_mapping)
    if args.storages_mapping:
        load_storage_id_to_type_mapping(args.storages_mapping)

    app.run(host=app.config['HOST'])
