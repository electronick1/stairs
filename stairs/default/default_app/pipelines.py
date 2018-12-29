
from .app_config import app

from .flows import FlowSample
from .consumers import output


@app.pipeline()
def main(pipeline, data_frame):
    return data_frame\
        .make(data=data_frame)\
        .subscribe_flow(FlowSample)\
        .subscribe_output(output)



