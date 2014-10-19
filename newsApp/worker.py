#background worker to do jobs

import time

from constants import *
from loggingHelper import *
from jobManager import JobManager
from workerJob import WorkerJob

def DequeueAndExecuteJob():
    """
    Dequeue a job from the queue and start executing it.
    """

    logging.info("Dequeing a job.");
    jobManager = JobManager()
    job = jobManager.dequeueJob()

    if job is None:
        logging.info("No job found. Sleeping for 10 seconds")
        time.sleep(10)
        return

    logging.info(
        "Job found. Job Name: %s. Job Params: %s.",
        job.jobName,
        str(job.jobParams))
    #code to execute job to be written here.

# keep looping over jobs and executing them
if __name__ == '__main__':
    InitLogging()
    while (True):
        DequeueAndExecuteJob()
