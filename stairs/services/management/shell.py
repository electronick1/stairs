import click

from IPython.terminal.embed import InteractiveShellEmbed


@click.group()
def shell_cli():
    pass


@shell_cli.command("shell")
def init_session():
    """
    Start Ipython interactive shell with full stairs project environment.
    """
    shell = InteractiveShellEmbed()
    shell()
