from fabric.state import env
from fabric.network import disconnect_all
from pprint import pprint


class Job_Queue(object):

    def __init__(self, max_running):
        self._queued = []
        self._running = []
        self._completed = []
        self._num_of_jobs = 0
        self._max = max_running
        self._finished = False
        self._closed = False
        self._debug = False

    def _all_alive(self):
        return all([x.is_alive() for x in self._running])

    def close(self):
        if self._debug:
            print("job queue closed.")

        self._num_of_jobs = len(self._queued)
        self._closed = True

    def append(self, process):
        if not self._closed:
            if self._debug:
                print("job queue appended %s." % process.name)
            global env
            env.host_string = env.host = process.name
            self._queued.append(process)

    def start(self):

        def _advance_the_queue():
            job = self._queued.pop()
            env.host_string = env.host = job.name
            job.start()
            self._running.append(job)

        if not self._closed:
            raise Exception("Need to close() before starting.")

        if self._debug:
            print("Job queue starting.")
            print("Job queue intial running queue fill.")

        while len(self._running) < self._max:
            _advance_the_queue()

        while not self._finished:

            while len(self._running) < self._max and self._queued:
                if self._debug:
                    print("Job queue running queue filling.")
              
                _advance_the_queue()

            if not self._all_alive():
                for id, job in enumerate(self._running):
                    if not job.is_alive():
                        if self._debug:
                            print("Job queue found finished proc: %s." %
                                    job.name)
                        
                        done = self._running.pop(id)
                        self._completed.append(done)

                        if self._debug:
                            print("Job queue has %d running." % len(self._running))

            if not (self._queued or self._running):
                if self._debug:
                    print("Job queue finished.")

                for job in self._completed:
                    job.join()

                self._finished = True

        disconnect_all()


def test_Job_Queue():

    def print_number(number):
        print(number)

    from multiprocessing import Process

    jobs = Job_Queue(5)
    jobs._debug = True

    for x in range(20):
        jobs.append(Process(
            target = print_number,
            args = [x],
            kwargs = [],
            ))

    jobs.close()
    jobs.start()


if __name__ == '__main__':
    test_Job_Queue()
