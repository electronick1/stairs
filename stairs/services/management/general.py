import click

@click.group()
def general_cli():
    pass


@general_cli.command("admin")
def init_admin():
    from stairs.services.admin import server
    server.run_admin_server()
