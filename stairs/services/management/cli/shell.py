import os
from . import config
from . import magic_functions
from . ipython_hacks import get_python_matches
from . import namespace


def get_shell():
    def handle_exception(*args, **kwargs):
        print("Keyboard Interrupt")

    from IPython.terminal.embed import InteractiveShellEmbed
    ipshell = InteractiveShellEmbed(user_ns=namespace.get_namespace(),
                                    config=config.get_config(),
                                    banner1='Rock star mode activated. Press Ctrl-D to exit.',
                                    exit_msg='Leaving Interpreter, back to program.',
                                    custom_exceptions=((KeyboardInterrupt, ), handle_exception))

    ipshell.magics_manager.magics['line'] = {
        key: v for key, v in ipshell.magics_manager.magics['line'].items()
        if key != '%pinfo'
    }

    magic_functions.init(ipshell)

    ipshell.automagic = True

    ipshell.alias_manager.clear_aliases()

    ipshell.Completer.use_main_ns = False
    ipshell.Completer.use_jedi = False

    ipshell.Completer.matchers = [
                                     ipshell.Completer.python_func_kw_matches,
                                 ] + list(get_python_matches(ipshell.Completer))

    from IPython.core import magic

    magic.magics = {}

    return ipshell
