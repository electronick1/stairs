from stairs.core.producer.adapter import BaseProducerAdapter
from .jobs_manager import JobsManager


class IterWorkerAdapter(BaseProducerAdapter):

    app_input = None
    app_output = None

    handler = None

    jobs_manager = None

    def __init__(self, app, handler, app_input, jobs_manager=None):
        self.app = app
        self.handler = handler
        self.app_input = app_input

        if jobs_manager is None:
            jobs_manager = JobsManager(self, 3)

        self.jobs_manager = jobs_manager

    def init_session(self):
        """
        Init producer session
        """
        cursor_chunks_list = list(self.handler())
        self.jobs_manager.init_producer_workers(cursor_chunks_list)

    def process(self):
        cursor_chunks_list = list(self.handler())
        self.jobs_manager.chunks_producer_worker(cursor_chunks_list)
