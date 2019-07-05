from typing import List, Union
from stairs.core.producer import Producer
from stairs.core.pipeline import Pipeline


def on_pipeline_empty(producer: Producer):
    pipelines: List[Pipeline] = producer.default_callbacks

    empty_pipelines = 0

    for pipeline in pipelines:
        empty_pipelines += 1 if pipeline.is_empty() else 0

    all_pipelines_empty = empty_pipelines == len(pipelines)
    producer_empty_itself = producer.get_stepist_step().is_empty()

    return all_pipelines_empty and producer_empty_itself