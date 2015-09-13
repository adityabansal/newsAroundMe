from constants import *
from loggingHelper import *

from clusterManager import ClusterManager
from jobManager import JobManager
from workerJob import WorkerJob

InitLogging()

def pushClusterJob():
    """
    Push the cluster job to job queue once all pre-processing is done.
    """

    jobManager = JobManager()
    clusterManager = ClusterManager()

    jobCount = jobManager.count()
    logging.info("Got job count as: %i", jobCount)

    clusterState = clusterManager.getState()
    logging.info("Got cluster state as: %s", clusterState)

    if (jobCount == 0) & (clusterState == CLUSTER_STATE_NEW):
        clusterJob = WorkerJob(JOB_CLUSTERDOCS, {})
        jobManager.enqueueJob(clusterJob)
        logging.info("Cluster job put. Job id: %s", clusterJob.jobId)

if __name__ == '__main__':
    pushClusterJob()
