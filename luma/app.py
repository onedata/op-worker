from flask import Flask
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def create_app(config):
    app = Flask(__name__)
    app.config.from_pyfile(config)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///{0}'.format(
        app.config['DATABASE'])
    db.init_app(app)

    return app
