import click
from stairs import get_project
from multiprocessing import Process

@click.group()
def workers_cli():
    pass


@workers_cli.command("pipelines:run")
@click.argument('apps', nargs=-1)
@click.argument('concurrency', nargs=1, default=1)
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
def run(apps, noprint, concurrency):
    """
    Run workers process
    """
    project = get_project()
    project.set_verbose(not noprint)

    if project.verbose:
        print("Pipelines started")

    processes = []
    for i in range(concurrency):
        p = Process(target=project.run_pipelines)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
