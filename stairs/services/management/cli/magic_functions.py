import stepist

from stairs.core import app as apps_worker
from stairs.core.flow.step import StairsStepAbstract

from stepist.flow.session import get_steps_to_listen


def init(ipshell):
    ipshell.register_magic_function(producer,
                                    magic_kind='line',
                                    magic_name='producer')

    ipshell.register_magic_function(workers_run,
                                    magic_kind='line',
                                    magic_name='workers.run')


def producer(app_name_and_producer):
    if '.' not in app_name_and_producer:
        print("You should define producer for app `%s`" % app_name_and_producer)
        return

    app_name, producer = app_name_and_producer.split('.')

    print("Start generating jobs")
    app = apps_worker.get_app_by_name(app_name)
    app.components.producers[producer].process()


def workers_run(app_name):
    steps_to_process = []

    if not app_name:
        steps_to_process = get_steps_to_listen().values()
    else:
        for stepist_step in get_steps_to_listen().values():

            step = stepist_step.handler

            if isinstance(step, StairsStepAbstract):
                app = apps_worker.get_app(step.handler)
            else:
                app = apps_worker.get_app(step)

            if app.app_name in app_name:
                steps_to_process.append(stepist_step)

    print("START PROCESSING:")
    print(steps_to_process)

    stepist.run(*steps_to_process)
