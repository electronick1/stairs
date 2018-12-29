from contextlib import contextmanager

from stairs.core.session import local


def flow_storage():
    if not hasattr(local, 'local_flow'):
        local.local_flow = dict()

    return local.local_flow


def get_flow():
    return flow_storage().get("flow", None)


def get_step_result(step_key):
    s = flow_storage()

    return s.get('flow_steps', {}).get(step_key, None)


def get_result_steps():
    s = flow_storage()

    return s.get('result_steps', None)


def flush_step_results():
    s = flow_storage()
    s['flow_steps'] = {}


def set_flow(flow_instance):
    flow_storage()["flow"] = flow_instance


def set_flow_step(flow_step, step_data):
    s = flow_storage()

    if "flow_steps" not in s:
        s["flow_steps"] = dict()

    s['flow_steps'][flow_step.key()] = step_data


def set_result_steps(result_steps):
    s = flow_storage()
    s['result_steps'] = result_steps


@contextmanager
def change_result_steps(result_steps):
    prev_steps = get_result_steps()

    try:
        set_result_steps(result_steps)
        yield
    finally:
        set_result_steps(prev_steps)


@contextmanager
def change_flow_ctx(flow):
    prev_flow = get_flow()

    try:
        set_flow(flow)
        yield
    finally:
        set_flow(prev_flow)
