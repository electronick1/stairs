

class SimpleQueueStats:

    def __init__(self, name, amount_of_jobs, amount_of_time, jobs_processed):
        self.name = name
        self.amount_of_jobs = amount_of_jobs
        self.jobs_processed = jobs_processed
        self.amount_of_time = amount_of_time

    def get_jobs_per_sec(self):
        return self.jobs_processed / self.amount_of_time

    def to_dict(self):
        return dict(
            name=self.name,
            amount_of_jobs=self.amount_of_jobs,
            jobs_per_sec=self.get_jobs_per_sec(),
        )

    def print_info(self):
        print("Queue: %s" % self.name)
        print("Amount of jobs: %s" % self.amount_of_jobs)
        jobs_per_sec = self.get_jobs_per_sec()
        if jobs_per_sec > 0:
            print("Queue growing by %s tasks per/sec" % jobs_per_sec)
        else:
            print("Queue decreasing by %s tasks per/sec" % jobs_per_sec)


class MonitorQueueStats(SimpleQueueStats):

    def __init__(self, new_jobs, *args, **kwargs):
        self.new_jobs = new_jobs

        SimpleQueueStats.__init__(self, *args, **kwargs)

    def print_info(self):
        print("Queue: %s" % self.name)
        print("Amount of jobs: %s" % self.amount_of_jobs)

        print("New jobs per/sec: %s" % (self.new_jobs/self.amount_of_time))
        print("Jobs processed per/sec: %s" % (self.jobs_processed/self.amount_of_time))
