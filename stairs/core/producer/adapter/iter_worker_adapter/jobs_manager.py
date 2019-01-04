import ujson
import random
import time

from stepist.flow.steps.step import StepData


BITMAP_EXPIRE_TIME = 60  # 1 minute


class JobsManager(object):

    def __init__(self, adapter, max_chunks_online, max_jobs_per_input=1000000):
        self.adapter = adapter
        self.max_chunks_online = max_chunks_online

        self.max_jobs_per_input = max_jobs_per_input

    def init_producer_workers(self, cursor_chunks_list):
        """
        Add jobs with chunks id, which should be processed.
        """
        redis_db = self.adapter.app.dbs.redis_db

        count = len(cursor_chunks_list)

        redis_db.setbit(self.worker_checker_key(), count, 0)

    def chunks_producer_worker(self, cursor_chunks_list):
        """
        Start's reading process.
        """

        redis_db = self.adapter.app.dbs.redis_db
        amount_of_chunks = len(cursor_chunks_list)

        while True:
            # getting random chunks
            chunk_index = random.randint(0, amount_of_chunks - 1)

            pipe = redis_db.pipeline()
            # check if we already processed this chunk
            pipe.getbit(self.worker_checker_key(), chunk_index)
            pipe.exists(self.worker_checker_key())

            bit, exist = pipe.execute()

            if int(bit) == 1:
                # checking if we already processed all chunks
                chunks_done = redis_db.bitcount(self.worker_checker_key())
                if chunks_done >= amount_of_chunks:
                    # if we processed everything. Let's remove bits map.
                    redis_db.delete(self.worker_checker_key())
                    print("generator done")
                    exit()
                continue

            if int(exist) == 0:
                print("looks like generator done")
                exit()

            queue_key = self.get_queue_key()

            # trying to get client's list names and set redis name.
            pipe = redis_db.pipeline()
            pipe.client_list()
            pipe.client_setname("chunk_%s" % chunk_index)
            pipe.llen(queue_key)
            clients_list, _, queue_len = pipe.execute()

            # if we already have client which working on current chunk
            # continue workflow
            if "chunk_%s" % chunk_index in [c['name'] for c in clients_list]:
                continue

            if queue_len > self.max_jobs_per_input:
                print("to much jobs, sleeping .. ")
                time.sleep(20)
                continue

            iter = cursor_chunks_list[chunk_index]

            pipe = redis_db.pipeline()

            for row in iter:
                step_data = StepData(flow_data=row)
                data = dict(data=step_data.get_dict())
                pipe.lpush(queue_key, ujson.dumps(data))

            pipe.setbit(self.worker_checker_key(), chunk_index, 1)
            pipe.execute()

    def worker_checker_key(self):
        return "worker_generator:%s" % self.get_queue_key()

    def get_queue_key(self):
        return "stepist::job::%s" % self.adapter.app_input.step.step_key()

    def chunk_worker_key(self,):
        return "%s:chunk_worker" % self.adapter.get_redis_key(),

    def chunk_key(self, chunk_id):
        return "%s:chunk:%s" % (self.adapter.get_redis_key(),
                                chunk_id)
