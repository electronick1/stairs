import click
import stepist

from stairs.core import app as apps_worker
from stairs.core.flow.step import StairsStepAbstract

from stairs.core.session import project_session

from stepist.flow.session import get_steps_to_listen, get_step_by_key
from stairs.core.output.standalone import StandAloneConsumer


@click.group()
def workers_cli():
    pass


@workers_cli.command("workers:run")
@click.argument('apps', nargs=-1)
def run(apps):
    """
    Run workers process
    """

    project = project_session.get_project()

    components_to_run = []

    for component in project.stepist_app.get_workers_steps():
        if not isinstance(component.handler, StandAloneConsumer):
            components_to_run.append(component)

    project.stepist_app.run(components_to_run)
