from stairs.core.flow import Flow, step


class FlowSample(Flow):

    def __call__(self, data):
        r = self.start_from(self.first_step, value=data)

        return dict(output=r.second_step)

    @step(None)
    def second_step(self, value2):
        return dict(value3=value2)

    @step(second_step)
    def first_step(self, value):
        return dict(value2=value)
