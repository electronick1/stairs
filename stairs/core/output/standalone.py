from stairs.core.output.output_model import Output


class StandAloneConsumer(Output):

    def __init__(self, *args, **kwargs):
        Output.__init__(self, *args, **kwargs, as_daemon=True)

    def __call__(self, *args, **kwargs):
        pass
