import stepist

from stepist.flow import session
from stepist.flow.steps.next_step import call_next_step

from stairs.core.producer.adapter import BaseProducerAdapter


class IterAdapter(BaseProducerAdapter):

    # db cursor
    handler = None

    app_input = None

    def __init__(self, app, handler, app_inputs, custom_inputs):
        self.app = app
        self.app_inputs = app_inputs or []
        self.custom_inputs = custom_inputs or dict()
        self.handler = handler

    def init_session(self):
        """
        Init generator session
        """
        pass

    def process(self, custom_pipelines_keys=None):
        try:
            for row in self.handler():
                with session.change_flow_ctx({}, {}):
                    if not isinstance(row, dict):
                        row = {'row': row}

                    pipelines_to_run = list(self.app_inputs)

                    for pipeline_key in custom_pipelines_keys or []:
                        if pipeline_key not in self.custom_inputs:
                            print("Pipeline %s not found" % pipeline_key)
                            print("Here available pipelines:")
                            self.print_custom_pipelines()
                        else:
                            pipelines_to_run.append(self.custom_inputs[pipeline_key])

                    if not pipelines_to_run:
                        print("No pipelines specified for producer")
                        print("If you have custom pipelines list them in command")

                    for input in pipelines_to_run:
                        call_next_step(row, input.step)

        except Exception:
            raise
    
    def flush_all(self):
        for input in self.app_inputs:
            input.step.flush_all()

    def print_custom_pipelines(self):
        for p in self.custom_inputs:
            print(p)
