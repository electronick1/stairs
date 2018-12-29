from . import global_stairs


def storage():
    if not hasattr(global_stairs, 'project'):
        global_stairs.project = dict(
            project=None,
        )

    return global_stairs.project


def set_project(project):
    storage()['project'] = project


def get_project():
    return storage()['project']
