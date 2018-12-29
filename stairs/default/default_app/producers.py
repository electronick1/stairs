from .app_config import app
from . import pipelines


@app.producer(pipelines.main)
def generate_data():
    for i in range(20):
        yield i


@app.worker_producer(pipelines.main, None)
def generate_data_worker():
    return [map(lambda x: 1, [1, 2, 3])]
