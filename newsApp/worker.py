#background worker to do jobs

import time
import threading

from constants import *
from loggingHelper import *
from jobManager import JobManager
from workerJob import WorkerJob
from rssProcessor import *
from linkProcessor import *
import clusteringJobs as cj

def RunJob(job):
    "Run a job taking care of error handling."

    try:
        if job.jobName == JOB_PROCESSFEED:
            processFeed(job.jobId, job.jobParams[JOBARG_PROCESSFEED_FEEDID])
        if job.jobName == JOB_PROCESSLINK:
            processLink(job.jobId, job.jobParams[JOBARG_PROCESSLINK_LINKID])
        if job.jobName == JOB_PARSEDOC:
            cj.parseDoc(job.jobId, job.jobParams[JOBARG_PARSEDOC_DOCID])
        if job.jobName == JOB_GETCANDIDATEDOCS:
            cj.getCandidateDocs(
                job.jobId,
                job.jobParams[JOBARG_GETCANDIDATEDOCS_DOCID])
        if job.jobName == JOB_COMPAREDOCS:
            cj.compareDocs(
                job.jobId,
                job.jobParams[JOBARG_COMPAREDOCS_DOC1ID],
                job.jobParams[JOBARG_COMPAREDOCS_DOC2ID])
        if job.jobName == JOB_CLUSTERDOCS:
            cj.clusterDocs(job.jobId)
    except:
        logging.exception('')

class JobThread(threading.Thread):
   def __init__ (self, job):
      threading.Thread.__init__(self)
      self.job = job
   def run(self):
      RunJob(self.job)

def DequeueAndStartJob():
    """
    Dequeue a job from the queue and start executing it.
    """

    logging.info("Dequeing a job.");
    jobManager = JobManager()
    job = jobManager.dequeueJob()

    if job is None:
        logging.info("No job found. Sleeping for 30 seconds")
        time.sleep(30)
        return

    logging.info(
        "Job found. Starting it now." + \
        "Job id: %s. Job Name: %s. Job Params: %s.",
        job.jobId,
        job.jobName,
        str(job.jobParams))
    jobThread = JobThread(job)
    jobThread.start()

MAX_JOB_THREADS = 10

if __name__ == '__main__':
    InitLogging()
    while (True):
        #subtracting 1 because current parent thread is also counted
        nThreads = threading.activeCount() - 1
        logging.info("No of threads are: %i", nThreads)

        if nThreads < MAX_JOB_THREADS:
            DequeueAndStartJob()
        else:
            logging.info("Too many threads. Sleeping")
            time.sleep(5)
