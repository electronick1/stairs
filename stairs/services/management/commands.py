# import os
import click

from stairs.services.management.project import project_cli
from stairs.services.management.workers import workers_cli
from stairs.services.management.producer import producer_cli
from stairs.services.management.general import general_cli
from stairs.services.management.consumer import consumer_cli


def init_cli():
    cli = click.CommandCollection(sources=[producer_cli,
                                           workers_cli,
                                           general_cli,
                                           project_cli,
                                           consumer_cli])
    cli()


if __name__ == "__main__":
    init_cli()
