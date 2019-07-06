import click

from stairs.core.producer import run_jobs_processor
from stairs.core.producer import Producer
from stairs.core.producer.batch import BatchProducer
from stairs.core.producer.spark import SparkProducer
from stairs import get_project


@click.group()
def producer_cli():
    pass


@producer_cli.command("producer:run")
@click.argument('name')
@click.option('--noprint', '-np', is_flag=True, help="Disable print", default=False)
@click.option('--nobatch_reading', '-nb', is_flag=True, help="Disable auto reading", default=False)
def run(name, noprint, nobatch_reading):
    """
    Run your producer.

    In case you are running simple producer will generate jobs to pipelines and
    automatically exit.

    In case you are running batch producer will generate batches and automatically
    start batch reading process. To disable auto-reading specify --nobatch_reading
    flag.
    """
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Init producer session.")

    producer = get_project().get_producer_by_name(name)
    if producer is None:
        raise RuntimeError("Producer not found")

    if isinstance(producer, Producer):
        producer()
        return

    if isinstance(producer, BatchProducer):
        producer(*[], **{})
        if project.verbose:
            print("Batch producer finished batch generation")
        if not nobatch_reading:
            if project.verbose:
                print("Starting batches reading process "
                      "and listening for a jobs ... ")
                print("Press Ctrl-C to cancel listening")
            batch_handler = producer.producer
            run_jobs_processor(get_project(),
                               [batch_handler])

        return

    if isinstance(producer, SparkProducer):
        import time
        t1 = time.time()
        producer()
        t2 = time.time()
        print(t2 - t1)
        return

    raise RuntimeError("Producer `%s` not found or not supported" % name)


@producer_cli.command("producer:run_jobs")
@click.argument("name", default=None)
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def run_jobs(name, pipelines, noprint):
    """
    Run jobs which were generated by batch producer. You can apply this command
    both for simple producer and batch producer.
    """
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Starting batches reading process  and listening for a jobs ...")
        print("Press Ctrl-C to cancel listening")

    producers = []

    if name is None:
        for app in get_project().apps:
            producers = producers + app.components.producers.values()
    else:
        producer = get_project().get_producer_by_name(name)
        if producer is None:
            raise RuntimeError("Producer not found")

        if isinstance(producer, Producer):
           producers.append(producer)

        if isinstance(producer, BatchProducer):
            producers.append(producer.producer)

    if not producers:
        print("No producers found")

    run_jobs_processor(get_project(),
                       producers)


@producer_cli.command("producer:flush_all")
@click.argument("name")
@click.option('--noconfirm', '-nс', is_flag=False,
              help="No confirmation message")
def flush_all(name, noconfirm):
    """
    Flush all jobs which were generated by specified producer.
    """
    if not noconfirm:
        msg = "Do you want to flush all jobs in %s producer?" % name
        if click.confirm(msg):
            noconfirm = True

    if noconfirm:
        producer = get_project().get_producer_by_name(name)
        if producer is None:
            raise RuntimeError("Producer not found")
        producer.flush()

    return


@producer_cli.command("producers")
def list_of_producers():
    """
    Returns producers list used in your project.
    """
    for app in get_project().apps:
        for producer_name in app.components.producers:
            print("%s.%s" % (app.app_name, producer_name))

