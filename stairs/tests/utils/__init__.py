from stepist import flow as stepist_flow


def run_stepist_workers():
    processes = stepist_flow.just_do_it(1)
    import time
    time.sleep(2)
    [p.terminate() for p in processes]
