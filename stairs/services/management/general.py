import click
from stairs.core import app

@click.group()
def general_cli():
    pass


@general_cli.command("run")
@click.argument('name')
def init_session(name):
    app_name, command_name = name.split('.')
    app.get_app_by_name(app_name).get_command(command_name)()


@general_cli.command("cli")
def init_session():
    shell.get_shell()("Let's go")


# @general_cli.command("admin")
# def init_admin():
#
#     admin_server.run_admin_server()
