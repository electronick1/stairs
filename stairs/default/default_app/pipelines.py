
from .app_config import app

from .flows import FlowSample
from .consumers import print_smth

from stairs import PipelineInfo, DataPoint, DataFrame


@app.pipeline()
def main(pipeline: PipelineInfo, data_frame: DataPoint) -> DataFrame:
    """
    Pipeline which apply some functions (flows) to your data.

    More info: http://stairspy.com/#pipeline

    """
    return data_frame\
        .rename(data=data_frame)\
        .subscribe_flow(FlowSample())\
        .subscribe_consumer(print_smth)



