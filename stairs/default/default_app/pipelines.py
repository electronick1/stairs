from .app_config import app

from .flows import calculate_fibonacci
from .consumers import print_fib

from stairs import PipelineInfo, DataPoint, DataFrame


@app.pipeline()
def main(pipeline: PipelineInfo, data_point: DataPoint) -> DataFrame:
    """
    Pipeline which apply functions to calculate fibonacci for your data.
    More info: http://stairspy.com/#pipeline
    """
    return (data_point
            .rename(value=data_point)
            .subscribe_func(calculate_fibonacci)
            .subscribe_consumer(print_fib))
