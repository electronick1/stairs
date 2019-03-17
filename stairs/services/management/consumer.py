import click
from stairs import get_project


@click.group()
def consumer_cli():
    pass


@consumer_cli.command("consumer:standalone")
@click.argument('name')
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def init_session(name, noprint):
    """
    Start listening pipelines and then execute jobs which were sent to specified
    consumer.
    """
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Standalone consumer started, and listening for a tasks ...")
        print("Press CTRL-C to stop listening")

    consumer = get_consumer_by_name(name)
    consumer.run_worker()


def get_consumer_by_name(name):
    if '.' in name:
        app_name, consumer_name = name.split('.')
        user_app = get_project().get_app_by_name(app_name)
        return user_app.components.consumers[consumer_name]
    else:
        consumer_component = None
        for app in get_project().apps:
            if name in app.components.consumers:
                if consumer_component is not None:
                    print("There is more then one `%s` consumer found, "
                          "please specified app name: app.consumer_name")
                    exit()
                else:
                    # Keep consumer component, as we need to check
                    # all consumers for duplication
                    consumer_component = app.components.consumers[name]

        return consumer_component
