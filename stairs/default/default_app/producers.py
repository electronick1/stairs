from .app_config import app
from . import pipelines


@app.producer(pipelines.main)
def generate_data():
    for i in range(20):
        yield dict(data_frame=i)
