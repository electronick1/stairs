import os

from flask import Flask, url_for
from flask_mako import MakoTemplates
from flask_mako import render_template

from stairs.services.admin import stairs_config, flask_config

basedir = "/Users/oleg/programming/stairs/stairs/services/admin"

app = Flask(import_name='stairs.services.admin.server',
            root_path=flask_config.basedir,#basedir,
            static_folder='templates/static')

#app.config.from_object('stairs.services.admin.flask_config')

mako = MakoTemplates(app)

## Route to any template


@app.route('/')
def index():
    return render_template('main.mako',
                           apps=stairs_config.get_apps())


@app.route('/pipeline/<app_name>/<worker_key>')
def pipeline(app_name, worker_key):
    apps = stairs_config.get_apps()

    app = [app for app in apps if app.app_name == app_name]
    if len(app) != 1:
        raise RuntimeError("APP not found")

    app = app[0]

    worker = app.components.workers.get(worker_key, None)
    if worker is None:
        raise RuntimeError("Worker not found")

    return render_template('pipeline.mako',
                           apps=apps,
                           app=app,
                           worker=worker)


@app.route('/generator/<app_name>/<generator_key>')
def generator(app_name, generator_key):
    apps = stairs_config.get_apps()

    app = [app for app in apps if app.app_name == app_name]
    if len(app) != 1:
        raise RuntimeError("APP not found")

    app = app[0]

    producer = app.components.producers.get(generator_key, None)
    if producer is None:
        raise RuntimeError("Producers not found")

    return render_template('generator.mako',
                           apps=apps,
                           app=app,
                           producer=producer)


def run_admin_server():
    # run on port 5000 by default

    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
