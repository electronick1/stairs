import click
from stairs import get_project


@click.group()
def workers_cli():
    pass


@workers_cli.command("pipelines:run")
@click.argument('apps', nargs=-1)
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def run(apps, noprint):
    """
    Run workers process
    """
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Pipelines started")

    project.run_pipelines()
