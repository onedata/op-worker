#!/usr/bin/env python2
import argparse
import os

import luma.model
from luma.app import db, create_app

parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Init LUMA server database')

parser.add_argument(
        '-c', '--config',
        action='store',
        default='config.cfg',
        help='cfg file with app configuration',
        dest='config')

args = parser.parse_args()
db.create_all(app=create_app(os.path.join(os.getcwd(), args.config)))
