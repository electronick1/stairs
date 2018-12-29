from stairs.core.output.output_model import Output


class Savepoint(Output):

    def __init__(self):
        Output.__init__(self, self.write, as_daemon=True)

    def read(self):
        raise NotImplementedError()

    def write(self):
        raise NotImplementedError()
