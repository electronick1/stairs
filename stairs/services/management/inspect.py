import click

from stairs import get_project
from stairs.services.inspect.pipelines import get_from_monitor, get_all


@click.group()
def inspect_cli():
    pass


@inspect_cli.command("inspect:status")
@click.argument("app_name", default=None)
def init_session(app_name):
    project = get_project()

    if app_name:
        app = project.get_app_by_name(app_name)
        apps = [app]
    else:
        apps = get_project().apps

    for app in apps:
        inspection = get_all(app)
        for status in inspection:
            status.print_info()
            print("=========")


@inspect_cli.command("inspect:monitor")
@click.argument("app_name", default=None)
def init_session(app_name):
    project = get_project()

    if app_name:
        app = project.get_app_by_name(app_name)
        apps = [app]
    else:
        apps = get_project().apps

    for app in apps:
        inspection = get_from_monitor(app)
        for status in inspection:
            status.print_info()
            print("=========")
