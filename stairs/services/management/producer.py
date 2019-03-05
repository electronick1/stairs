import click
from stairs import get_project


@click.group()
def producer_cli():
    pass


@producer_cli.command("producer:init")
@click.argument('name')
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def init_session(name, noprint):
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Init producer session.")

    producer = get_producer_by_name(name)
    producer()


@producer_cli.command("producer:run")
@click.argument("name")
@click.argument('pipelines', nargs=-1)
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def process(name, pipelines, noprint):
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Producer started.")

    producer = get_producer_by_name(name)
    producer.process(pipelines or [])


@producer_cli.command("producer:flush_all")
@click.argument("name")
@click.option('--noconfim', '-nс', is_flag=False, 
              help="No confirmation message")
def flush_all(name, noconfirm):
    if not noconfirm:
        msg = "Do you want to flush all jobs in %s producer?" % name
        if click.confirm(msg):
            noconfirm = True

    if noconfirm:
        producer = get_producer_by_name(name)
        producer.flush_all()

    return
    

# Utils:

def get_producer_by_name(name):
    if '.' in name:
        app_name, producer_name = name.split('.')
        user_app = get_project().get_app_by_name(app_name)
        return user_app.components.producers[producer_name]
    else:
        producer_component = None
        for app in get_project().apps:
            if name in app.components.producers:
                if producer_component is not None:
                    print("There is more then one `%s` producer found, "
                          "please specified app name: app.producer_name")
                    return None
                else:
                    # Keep producer component, as we need to check 
                    # all producers for duplication
                    producer_component = app.components.producers[name]

        return producer_component
