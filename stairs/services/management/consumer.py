import click
from stairs import get_project


@click.group()
def consumer_cli():
    pass


@consumer_cli.command("consumer:standalone")
@click.argument('name')
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def init_session(name, noprint):
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Standalone consumer started.")

    app_name, consumer_name = name.split('.')
    user_app = project.get_app_by_name(app_name)
    user_app.components.consumers[consumer_name].run_worker()
