from traitlets.config.loader import Config
from IPython.terminal.prompts import Prompts, Token


class CustomPrompt(Prompts):
    def in_prompt_tokens(self, cli=None):
        return [
            (Token.Prompt, '> '),
        ]

    def out_prompt_tokens(self):
        return [
            (Token.OutPrompt, '= '),
        ]


def get_config():
    nested = 0
    cfg = Config()
    cfg.TerminalInteractiveShell.prompts_class = CustomPrompt

    return cfg

