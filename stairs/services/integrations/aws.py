from stairs import get_project


def handle_sqs_event(pipeline, event):
    pickler = get_project().data_pickler

    for record in event:
        handle_job(pipeline, **pickler.loads(record.body))


def handle_job(pipeline, **data):
    pipeline.step.receive_job(**data)
    get_project().run_pipelines(pipeline,
                                die_when_empty=True,
                                die_on_error=True)
