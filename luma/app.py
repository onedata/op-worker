from flask_sqlalchemy import SQLAlchemy
from flask import Flask
from plugins_loader import PluginsLoader

import argparse

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Start LUMA server')

parser.add_argument(
    '-cm', '--credentials-mapping',
    action='store',
    default=None,
    help='json file with array of credentials mappings',
    dest='credentials_mapping_file')

parser.add_argument(
    '-gm', '--generators-mapping',
    action='store',
    default=None,
    help='json file with array of storages to generators mappings',
    dest='generators_mapping')

parser.add_argument(
    '-sm', '--storages-mapping',
    action='store',
    default=None,
    help='json file with array of storage id to type mappings',
    dest='storages_mapping')

parser.add_argument(
    '-c', '--config',
    action='store',
    default='config.cfg',
    help='cfg file with app configuration',
    dest='config')

args = parser.parse_args()

app = Flask(__name__)
app.config.from_pyfile(args.config)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///%s' % app.config['DATABASE']
db = SQLAlchemy(app)

plugins = PluginsLoader()
plugins.load_plugins()


