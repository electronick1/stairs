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
            pipelines_to_run.append(get_pipeline_by_name(p))
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


def get_pipeline_by_name(name):
    if '.' in name:
        app_name, pipeline_name = name.split('.')
        user_app = get_project().get_app_by_name(app_name)
        return user_app.components.pipelines[pipeline_name]
    else:
        pipeline_component = None
        for app in get_project().apps:
            if name in app.components.pipelines:
                if pipeline_component is not None:
                    print("There is more then one `%s` pipeline found, "
                          "please specified app name: app.pipeline_name")
                    exit()
                else:
                    # Keep pipeline component, as we need to check
                    # all pipelines for duplication
                    pipeline_component = app.components.pipelines[name]

        return pipeline_component
