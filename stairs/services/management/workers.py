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


@workers_cli.command("pipelines:run")
@click.argument('apps', nargs=-1)
def run(apps):
    """
    Run workers process
    """

    project = project_session.get_project()
    project.run_pipelines()
