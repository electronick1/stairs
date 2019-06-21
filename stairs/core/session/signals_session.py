from . import global_stairs


def storage():
    if not hasattr(global_stairs, 'signals'):
        global_stairs.signals = dict()

    return global_stairs.signals


def add_signal(key, signal):
    storage().setdefault(key, [])
    storage()[key].append(signal)


def get_signals(key):
    return storage().get(key, [])
