import os

from queueHelper import *
from workerJob import WorkerJob

class JobManager:
    """
    Helper class to enqueue and dequeue jobs to the job queue.
    """

    def __init__(self):
        """
        Instantiates the job manager.

        Following environment variables need to be set -
        'JOBSQUEUE_CONNECTIONSTRING' : connection string of jobs queue.
        """

        self.queue = getQueue(os.environ['JOBSQUEUE_CONNECTIONSTRING'])

    def enqueueJob(self, job):
        """
        Enqueue the job into the jobs queue.
        """

        enqueueMessage(self.queue, job.serializeToString())

    def dequeueJob(self):
        """
        Dequeue a job from the job queue.
        """

        dequeuedMessage = dequeueMessage(self.queue)
        if dequeuedMessage is None:
             return None
        dequeuedJob = WorkerJob(None, None);
        dequeuedJob.deserializeFromString(dequeuedMessage);
        return dequeuedJob


