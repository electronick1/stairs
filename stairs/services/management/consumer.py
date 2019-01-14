import click
from stairs.core.session import project_session


@click.group()
def consumer_cli():
    pass


@consumer_cli.command("consumer:standalone")
@click.argument('name')
def init_session(name):
    project = project_session.get_project()
    print("init standalone consumer session")
    app_name, consumer_name = name.split('.')
    user_app = project.get_app_by_name(app_name)
    user_app.components.consumers[consumer_name].run_worker()
