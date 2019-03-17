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

    consumer = get_project().get_consumer_by_name(name)

    if consumer is None:
        raise RuntimeError("Consumer not found")

    consumer.run_worker()

