from stairs.core.utils import AttrDict
from stepist.app_config import AppConfig as StepistConfig


class ProjectConfig(StepistConfig):
    # list of apps modules path

    @classmethod
    def init_default(cls):
        stepist_config = StepistConfig.init_default()
        return stepist_config

    @classmethod
    def load_from_file(cls, filename):
        """Load a config from a python file."""

        if isinstance(filename, str):
            d = dict()

            with open(filename) as f:
                code = compile(f.read(), filename, 'exec')
                exec(code, d)
        else:
            d = vars(filename)

        d = {key.lower(): value for key, value in d.items()}
        d = AttrDict(d)
        return d
