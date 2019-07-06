from typing import List
from stairs.core.producer import Producer
from stairs.core.pipeline import Pipeline

from stairs.core.session.project_session import get_project


def on_pipeline_empty(producer: Producer):
    pipelines: List[Pipeline] = producer.default_callbacks

    empty_pipelines = 0

    for pipeline in pipelines:
        empty_pipelines += 1 if pipeline.is_empty() else 0

    all_pipelines_empty = empty_pipelines == len(pipelines)
    producer_empty_itself = producer.get_stepist_step().is_empty()

    return all_pipelines_empty and producer_empty_itself


def on_all_components_empty(producer: Producer):
    all_empty = True

    for step in get_project().stepist_app.get_workers_steps():
        if not step.is_empty():
            all_empty = False

    return all_empty