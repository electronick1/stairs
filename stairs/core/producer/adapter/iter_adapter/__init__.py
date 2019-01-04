import stepist

from stepist.flow import session
from stepist.flow.steps.next_step import call_next_step

from stairs.core.producer.adapter import BaseProducerAdapter


class IterAdapter(BaseProducerAdapter):

    # db cursor
    handler = None

    app_input = None

    def __init__(self, app, handler, app_inputs):
        self.app = app
        self.app_inputs = app_inputs or []
        self.handler = handler

    def init_session(self):
        """
        Init generator session
        """
        pass

    def process(self):
        try:
            for row in self.handler():
                with session.change_flow_ctx({}, {}):
                    if not isinstance(row, dict):
                        row = {'row': row}

                    for input in self.app_inputs:
                        call_next_step(row, input.step)

        except Exception:
            raise
