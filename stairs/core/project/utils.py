from stairs.core.worker.pipeline_objects import PipelineComponent
from stairs.core.worker.pipeline_model import Pipeline

from stairs.core.consumer.standalone import StandAloneConsumer


PIPELINES_STEPS_TO_RUN = [
    PipelineComponent,
    Pipeline
]

STANDALONE_CONSUMERS_TO_RUN = [
    StandAloneConsumer
]
