import click
from stairs import get_project


@click.group()
def producer_cli():
    pass


@producer_cli.command("producer:init_session")
@click.argument('name')
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def init_session(name, noprint):
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Init producer session")

    if '.' in name:
        app_name, producer_name = name.split('.')
        user_app = project.get_app_by_name(app_name)
        user_app.components.producers[producer_name]()
    else:
        producer_component = None
        for app in get_project().apps:
            if name in app.components.producers:
                if producer_component is not None:
                    print("There is more then one `%s` producer found, "
                          "please specified app name: app.producer_name")
                    return
                else:
                    producer_component = app.components.producers[name]

        producer_component()


@producer_cli.command("producer:process")
@click.argument("name")
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def process(name, noprint):
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Producer started.")

    if '.' in name:
        app_name, producer_name = name.split('.')
        user_app = project.get_app_by_name(app_name)
        user_app.components.producers[producer_name].process()
    else:
        producer_component = None
        for app in get_project().apps:
            if name in app.components.producers:
                if producer_component is not None:
                    print("There is more then one `%s` producer found, "
                          "please specified app name: app.producer_name")
                    return
                else:
                    producer_component = app.components.producers[name]

        producer_component.process()
