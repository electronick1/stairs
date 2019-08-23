import click

from stairs import get_project
from stairs.services.inspect.pipelines import get_from_monitor, get_all


@click.group()
def inspect_cli():
    pass


@inspect_cli.command("inspect:jobs_count")
def inspect_jobs_count():
    steps = get_project().stepist_app.get_workers_steps()

    result = dict()

    for step in steps:
        step_name = step.step_key()\
            .replace("stepist::", "", 1)\
            .replace("stairs::", "", 1)

        result[step_name] = step.jobs_count()

    result = sorted(result.items(), key=lambda x: x[1], reverse=True)

    for row in result:
        print("->%s: %s" % row)


@inspect_cli.command("inspect:status")
@click.argument("app_name", default=None)
def inspect_status(app_name):
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
def inspect_monitor(app_name):
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
