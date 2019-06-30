from typing import List
from stairs.core.producer import Producer
from stairs.core.pipeline import Pipeline


def is_pipeline_empty(producer: Producer):
    pipelines: List[Pipeline] = producer.default_callbacks

    empty_pipelines = 0

    for pipeline in pipelines:
        empty_pipelines += 1 if pipeline.is_empty() else 0

    return empty_pipelines == len(pipelines)