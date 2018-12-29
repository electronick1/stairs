from stairs.core.utils import AttrDict


class Config(AttrDict):

    def load_python(self, filename):
        """Load a config from a python file."""
        if isinstance(filename, str):
            d = dict(self.__dict__)

            with open(filename) as f:
                code = compile(f.read(), filename, 'exec')
                exec(code, d)
        else:
            d = vars(filename)

        self.update(d, skip_reserved=True)

    def load(self, filename):
        """
        guess load type based on filename
        """
        # only python config available right now
        self.load_python(filename)


def init_project_config(filename):
    from stairs import config
    config.load(filename)


