import time
from stairs import get_project

from stairs.services.inspect.utils import SimpleQueueStats, MonitorQueueStats


def get_all(app):
    inspects = []

    for pipeline in app.components.pipelines.values():
        status = get_pipeline_status(pipeline)
        inspects.append(status)

    return inspects


def get_pipeline_status(pipeline, perf_iters_count=4, sleep_time_in_sec=2):
    elps_time = 0
    jobs_amount = 0

    jobs_list = []

    for i in range(perf_iters_count):
        # let's consider jobs count request super fast
        jobs_list.append(get_jobs_count(pipeline))
        if i != perf_iters_count - 1:
            time.sleep(sleep_time_in_sec)

    for i in range(1, perf_iters_count, 1):
        elps_time += sleep_time_in_sec
        jobs_amount += (jobs_list[i]-jobs_list[i-1])

    return SimpleQueueStats(name=pipeline.get_handler_name(),
                            amount_of_jobs=jobs_list[-1],
                            jobs_processed=jobs_amount,
                            amount_of_time=elps_time)


def get_jobs_count(pipeline):
    return get_project().stepist_app.worker_engine.jobs_count(pipeline.step)


def get_from_monitor(app, monitoring_for_sec=10):
    worker_engine = get_project().stepist_app.worker_engine

    steps = []
    for pipeline in app.components.pipelines.values():
        steps.append(pipeline.step)

    s_push, s_pop = worker_engine.monitor_steps(steps,
                                                monitoring_for_sec=monitoring_for_sec)

    inspects = []

    for pipeline in app.components.pipelines.values():
        step_key = pipeline.step.step_key()

        new_jobs = s_push.get(step_key, 0)
        jobs_processed = s_pop.get(step_key, 0)

        s = MonitorQueueStats(name=pipeline.get_handler_name(),
                              amount_of_jobs=get_jobs_count(pipeline),
                              jobs_processed=jobs_processed,
                              amount_of_time=monitoring_for_sec,
                              new_jobs=new_jobs)
        inspects.append(s)

    return inspects
