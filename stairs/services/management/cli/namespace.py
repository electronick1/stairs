from stairs.core.session import project_session


class CliApp(object):
    producers = []


def get_namespace():
    cli_apps = {}
    for app in project_session.get_apps():
        cli_app = CliApp()
        for name, handler in app.components.producers.items():
            setattr(cli_app, name, handler)
            cli_app.producers.append(name)

        cli_apps[app.app_name] = cli_app

    return cli_apps
