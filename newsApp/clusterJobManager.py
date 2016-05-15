from jobManager import JobManager

class ClusterJobManager(JobManager):
    """
    Helper class to enqueue and dequeue jobs to the cluster job queue.
    """

    def __init__(self):
        JobManager.__init__(self, 'CLUSTER_JOBSQUEUE_CONNECTIONSTRING')