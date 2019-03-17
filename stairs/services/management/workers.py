import click
from stairs import get_project
from multiprocessing import Process


@click.group()
def workers_cli():
    pass


@workers_cli.command("pipelines:run")
@click.argument('apps', nargs=-1)
@click.option('--processes', '-p', nargs=1, default=1)
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def run(apps, noprint, processes):
    """
    Run workers process
    """
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Pipelines started")

    processes_objects = []
    for i in range(processes):
        p = Process(target=project.run_pipelines)
        p.start()
        processes_objects.append(p)

    for p in processes_objects:
        p.join()
