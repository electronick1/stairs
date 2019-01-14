from stairs.core.worker.pipeline_objects import PipelineComponent
from stairs.core.worker.worker import Worker

from stairs.core.output.standalone import StandAloneConsumer

PIPELINES_STEPS_TO_RUN = [
    PipelineComponent,
    Worker
]

STANDALONE_CONSUMERS_TO_RUN = [
    StandAloneConsumer
]

