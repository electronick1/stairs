from .app_config import app
from . import pipelines


@app.producer(pipelines.main)
def generate_data():
    """
    Simple producer which yields data to `main` pipeline.
    More information here: http://stairspy.com/#producer
    """
    for i in range(20):
        yield dict(data_frame=i)
