import click

from stairs.core.producer.producer import run_jobs_processor
from stairs.core.producer.producer import Producer
from stairs.core.producer.batch_producer import BatchProducer
from stairs.core.producer.spark_producer import SparkProducer
from stairs import get_project


@click.group()
def producer_cli():
    pass


@producer_cli.command("producer:run")
@click.argument('name')
@click.argument('pipelines', nargs=-1)
@click.option('--noprint', '-np', is_flag=True, help="Disable print", default=False)
@click.option('--nobatch_reading', '-nb', is_flag=True, help="Disable auto reading", default=False)
def init_session(name, pipelines, noprint, nobatch_reading):
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Init producer session.")

    producer = get_producer_by_name(name)

    if isinstance(producer, Producer):
        producer.run(pipelines or [], user_args=[], user_kwargs={})
        return

    if isinstance(producer, BatchProducer):
        producer(*[], **{})
        if project.verbose:
            print("Batch producer finished batch generation")
        if not nobatch_reading:
            if project.verbose:
                print("Starting batches reading process ... ")
            batch_handler = producer.producer
            batch_handler.run_jobs_processor(pipelines)

        return

    if isinstance(producer, SparkProducer):
        import time
        t1 = time.time()
        producer.run(pipelines or [], user_args=[], user_kwargs={})
        t2 = time.time()
        print(t2 - t1)
        return

    raise RuntimeError("Producer `%s` not found or not supported" % name)


@producer_cli.command("producer:run_jobs")
@click.argument("name", default=None)
@click.argument('pipelines', nargs=-1)
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def process(name, pipelines, noprint):
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Producer started.")

    producers = []

    if name is None:
        for app in get_project().apps:
            producers = producers + app.components.producers.values()
    else:
        producer = get_producer_by_name(name)

        if isinstance(producer, Producer):
           producers.append(producer)

        if isinstance(producer, BatchProducer):
            producers.append(producer.producer)

    if not producers:
        print("No producers found")

    run_jobs_processor(get_project(),
                       producers,
                       custom_callbacks_keys=pipelines)


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
        producer.flush()

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
                    exit()
                else:
                    # Keep producer component, as we need to check 
                    # all producers for duplication
                    producer_component = app.components.producers[name]

        return producer_component
