from jobManager import JobManager

class MinerJobManager(JobManager):
    """
    Helper class to enqueue and dequeue jobs to the miner job queue.
    """

    def __init__(self):
        JobManager.__init__(self, 'MINER_JOBSQUEUE_CONNECTIONSTRING')