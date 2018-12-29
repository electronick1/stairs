import os


def name_by_module(module):
    return os.path.abspath(module.__file__)
