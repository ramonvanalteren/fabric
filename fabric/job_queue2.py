__author__ = 'ramon'

import multiprocessing
import logging
from fabric.network import disconnect_all

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

class Consumer(multiprocessing.Process):
    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        procname = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task == None:
                log.info("Consumer exiting, found my poison pill")
                self.task_queue.task_done()
                break
            # Break the tuple up in parts
            name, task, args, kwargs = next_task
            log.info("Starting on task: %s" % name)
            self.name = name
            result = task(*args, **kwargs)
            log.debug("result: %s" % dir(result))
            self.task_queue.task_done()
            self.result_queue.put({name: result})

class Job_Queue(object):

    def __init__(self, max_running):
        self._queued = multiprocessing.JoinableQueue()
        self._results = multiprocessing.Queue()
        self._closed = False
        self._debug = False
        self._max = max_running
        self._num_of_jobs = 0

    def close(self):
        self._closed = True

    def append(self, task, timeout=None):
        if not self._closed:
            self._queued.put(task, timeout=timeout)
            self._num_of_jobs += 1
            name, process, args, kwargs = task
            log.debug("Added job: %s to queue" % name)

    def start(self):
        if self._num_of_jobs < self._max:
            consumer_count = self._num_of_jobs
        else:
            consumer_count = self._max
        # Add stop pill in queue and start workers
        for _ in xrange(consumer_count):
            self._queued.put(None)
        consumers = [ Consumer(self._queued, self._results)
                     for i in xrange(consumer_count) ]
        for c in consumers:
            c.start()
        # Wait for the queue to empty
        self._queued.join()

        disconnect_all()

        # Return the result
        res = {}
        while self._num_of_jobs:
            res.update(self._results.get())
            self._num_of_jobs -= 1
        return res

def return_a_number(number):
    return "This is the number: %s" % number

def try_using():
    jobs = Job_Queue(5)
    for x in xrange(20):
        jobs.append(("host%s" % x, return_a_number, [x], {}))
    jobs.close()
    res = jobs.start()
    print "This is the result dict"
    print res


if __name__ == "__main__":
    try_using()



        

