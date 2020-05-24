import os

from .queueHelper import *
from .workerJob import WorkerJob

class JobManager:
    """
    Helper class to enqueue and dequeue jobs to the job queue.
    """

    def __init__(self, connectionStringKey):
        """
        Instantiates the job manager.

        'connectionStringKey' : name of environment variable containing the
        connection string to use.
        """

        self.queue = getQueue(os.environ[connectionStringKey])

    def enqueueJob(self, job):
        """
        Enqueue the job into the jobs queue.
        """

        enqueueMessage(self.queue, job.serializeToString())

    def convertDequeuedMessageToJob(self, dequeuedMessage):
        if dequeuedMessage is None:
            return None
        dequeuedJob = WorkerJob(None, None)
        dequeuedJob.deserializeFromString(dequeuedMessage)
        return dequeuedJob

    def dequeueJob(self):
        """
        Dequeue a job from the job queue.
        """

        dequeuedMessage = dequeueMessage(self.queue)
        return self.convertDequeuedMessageToJob(dequeuedMessage)

    def count(self):
        """
        Return the count of messages in queue.
        """

        return self.queue.count()
