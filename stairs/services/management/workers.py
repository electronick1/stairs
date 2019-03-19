import click
from stairs import get_project
from multiprocessing import Process


@click.group()
def workers_cli():
    pass


@workers_cli.command("pipelines:run")
@click.argument("pipelines", nargs=-1, default=None)
@click.option('--processes', '-p', nargs=1, default=1,
              help="Amount of processes to run in parallel")
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def run(pipelines, noprint, processes):
    """
    Run all or defined pipelines. Process listening for a jobs until you
    press CTRL-C to exit.

    Use `pipelines:run` to run all pipelines.
    Use `pipelines:run pipeline_name` to run specific pipeline.

    """
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Pipelines started")

    pipelines_to_run = []
    if pipelines is not None:
        for p in pipelines:
            p_to_run = get_project().get_pipeline_by_name(p)
            if p_to_run is None:
                raise RuntimeError("Pipeline `%s` not found" % p)
            pipelines_to_run.append(p_to_run)
    else:
        pipelines_to_run = None

    processes_objects = []
    for i in range(processes):
        p = Process(target=project.run_pipelines,
                    kwargs=dict(custom_pipelines_to_run=pipelines_to_run))
        p.start()
        processes_objects.append(p)

    for p in processes_objects:
        p.join()
