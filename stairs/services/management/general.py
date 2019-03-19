import click

from stairs import get_project

@click.group()
def general_cli():
    pass


@general_cli.command("admin")
def init_admin():
    from stairs.services.admin import server
    server.run_admin_server()


@general_cli.command("flushall")
def flushall():
    """
    Flush all queues which were used in the project.
    """
    p = get_project()

    msg = "Do you want to flush all jobs in your project?"
    if click.confirm(msg):
        for step in p.stepist_app.get_workers_steps():
            step.flush_all()
