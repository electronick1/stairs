import sys
import click
import subprocess
from stairs import get_project
from multiprocessing import get_context


@click.group()
def workers_cli():
    pass


@workers_cli.command("pipelines:run")
@click.argument("pipelines", nargs=-1, default=None)
@click.option('--processes', '-p', nargs=1, default=1,
              help="Amount of processes to run in parallel")
@click.option('--noprint', '-np', is_flag=True, help="Disable print")
@click.option('--is_fork', '-if', is_flag=True, default=False,
              help="If false fork could be applied")
@click.option('--booster', '-b', is_flag=True, default=False)
def run(pipelines, noprint, processes, is_fork, booster):
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

    spawn_context = get_context("fork")

    if project.stepist_app.use_booster:
        if not is_fork:
            processes_objects = []
            for i in range(processes):
                p = spawn_context.Process(target=exec_current_one)
                p.start()
                processes_objects.append(p)
                p = spawn_context.Process(target=exec_current_one,
                                          kwargs=dict(use_booster=True))
                p.start()
                processes_objects.append(p)

            for p in processes_objects:
                p.join()
        else:
            project.run_pipelines(pipelines_to_run, use_booster=booster)

    if processes > 1 and not is_fork:
        processes_objects = []
        for i in range(processes):
            p = spawn_context.Process(target=exec_current_one)
            p.start()
            processes_objects.append(p)

        for p in processes_objects:
            p.join()
    else:
        project.run_pipelines(pipelines_to_run, use_booster=booster)


def exec_current_one(use_booster=False):
    sys.argv.append("--is_fork")
    if use_booster:
        sys.argv.append("--booster")

    subprocess.call([sys.executable] + sys.argv)
