from stairs.core.pipeline.pipeline_objects import PipelineComponent
from stairs.core.pipeline import Pipeline

from stairs.core.app_components import AppBaseComponent
from stairs.core.consumer.standalone import StandAloneConsumer


PIPELINES_STEPS_TO_RUN = [
    PipelineComponent,
    Pipeline,
    AppBaseComponent
]

APP_STEPS_TO_RUN = [
    AppBaseComponent
]

EXCLUDE_PIPELINE_COMPONENTS = [
]

EXCLUDE_APP_COMPONENTS = [
    StandAloneConsumer
]


def is_step_related_to_pipelines(step):
    result = False

    handler = step.handler

    for component_to_run in PIPELINES_STEPS_TO_RUN:
        if isinstance(handler, component_to_run):
            result = True

    if isinstance(handler, PipelineComponent):
        for component_to_run in APP_STEPS_TO_RUN:
            if isinstance(handler.component, component_to_run):
                result = True

    for exclude_obj in EXCLUDE_PIPELINE_COMPONENTS:
        if isinstance(handler, exclude_obj):
            result = False

    if isinstance(handler, PipelineComponent):
        for exclude_obj in EXCLUDE_APP_COMPONENTS:
            if isinstance(handler.component, exclude_obj):
                result = False

    return result
