from stairs.core.flow import Flow, step


class FlowSample(Flow):
    """
    It's a way to run multiple steps at once.

    More info: http://stairspy.com/#flow
    """
    def __call__(self, data: dict) -> dict:
        # starts flow execution for `self.first_step`
        r = self.start_from(self.first_step, value=data)

        return dict(output=r.second_step)

    @step(None)
    def second_step(self, value2) -> dict:
        """
        Result of this step will be return to "caller".
        """
        return dict(value3=value2)

    @step(second_step)
    def first_step(self, value) -> dict:
        """
        Result of this step will be forwarded to `second_step`
        """
        return dict(value2=value)
