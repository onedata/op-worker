from app import db, create_app
import model
import argparse


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

if __name__ == "__main__":
    db.create_all(app=create_app(args.config))
