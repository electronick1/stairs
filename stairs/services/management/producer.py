import click
from stairs.core.session import project_session


@click.group()
def producer_cli():
    pass


@producer_cli.command("producer:init_session")
@click.argument('name')
def init_session(name):
    project = project_session.get_project()
    print("init producer session")
    app_name, producer_name = name.split('.')
    user_app = project.get_app_by_name(app_name)
    user_app.components.producers[producer_name]()


@producer_cli.command("producer:process")
@click.argument("name")
def process(name):
    project = project_session.get_project()
    print("init producer session")
    app_name, producer_name = name.split('.')
    user_app = project.get_app_by_name(app_name)
    print(user_app.components.producers)
    user_app.components.producers[producer_name].process()
