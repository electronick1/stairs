from stairs.core.session import project_session


def get_apps():
    print("GETTING APPS", project_session.get_apps())
    return project_session.get_apps()
