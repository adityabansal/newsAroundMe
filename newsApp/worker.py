#background worker to do jobs
import getopt
import os
import sys
import time
import threading
from multiprocessing import Process

from constants import *
from loggingHelper import *
from jobManager import JobManager
from workerJob import WorkerJob
from feedProcessor import *
from linkProcessor import *
import clusteringJobs as cj
import dbJobs as dj

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
        if job.jobName == JOB_UPDATEDBTHROUGHPUT:
            dj.updateDbThroughput(
                job.jobId,
                job.jobParams[JOB_UPDATEDBTHROUGHPUT_CONNECTIONSTRING],
                job.jobParams[JOB_UPDATEDBTHROUGHPUT_READTHOUGHPUT],
                job.jobParams[JOB_UPDATEDBTHROUGHPUT_WRITETHOUGHPUT],
                job.jobParams[JOB_UPDATEDBTHROUGHPUT_INDEXNAME])
        if job.jobName == JOB_CLEANUPDOC:
            cj.cleanUpDoc(job.jobId, job.jobParams[JOBARG_CLEANUPDOC_DOCID])
        if job.jobName == JOB_CLEANUPDOCSHINGLES:
            cj.cleanUpDocShingles(job.jobId, job.jobParams[JOBARG_CLEANUPDOCSHINGLES_DOCID])
        if job.jobName == JOB_CLEANUPDOCENTITIES:
            cj.cleanUpDocEntities(job.jobId, job.jobParams[JOBARG_CLEANUPDOCENTITIES_DOCID])
        if job.jobName == JOB_CLEANUPDOCDISTANCES:
            cj.cleanUpDocDistances(job.jobId, job.jobParams[JOBARG_CLEANUPDOCDISTANCES_DOCID])
        if job.jobName == JOB_PROCESSNEWCLUSTER:
            cj.processNewCluster(job.jobId, job.jobParams[JOBARG_PROCESSNEWCLUSTER_CLUSTER])
    except:
        logging.exception('Failed to execute worker job')

def DequeueAndStartJob(connectionStringKey):
    """
    Dequeue a job from the queue and start executing it.
    """

    logging.info("Dequeing a job.");
    jobManager = JobManager(connectionStringKey)
    job = jobManager.dequeueJob()

    if job is None:
        logging.info("No job found.")
        return

    logging.info(
        "Job found. Starting it now." + "Job id: %s. Job Name: %s.",
        job.jobId,
        job.jobName)
    RunJob(job)

class JobThread(threading.Thread):
   def __init__ (self, connectionStringKey):
      threading.Thread.__init__(self)
      self.connectionStringKey = connectionStringKey;
   def run(self):
      DequeueAndStartJob(self.connectionStringKey)

def RunWorker(connectionStringKey):
    InitLogging()
    while (True):
        jobManager = JobManager(connectionStringKey)
        jobCount = jobManager.count()

        # not spawning new thread for unless queue has a job
        # otherwise lot of SQS calls are made and bill is high
        if jobCount > 0:
            jobProcesses = []

            while len(jobProcesses) < int(os.environ['MAX_JOB_THREADS']):
                jobProcess = Process(target=DequeueAndStartJob, args=(connectionStringKey,))
                jobProcess.start()
                jobProcesses.append(jobProcess)

            logging.info("Process limit reached")

            for jobProcess in jobProcesses:
                jobProcess.join(150)
                if jobProcess.is_alive():
                    logging.warning("Process timed out. Terminating it")
                    jobProcess.terminate()
        else:
            logging.info("No job found in queue. Sleeping")
            time.sleep(5)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            opts, args = getopt.getopt(sys.argv[1:], "q:", ["queue"])
        except getopt.GetoptError as err:
            print str(err)
            sys.exit(2)
        for o, a in opts:
            if o in ("-q", "--queue"):
                RunWorker(a)
                sys.exit()

    print("Specify the queue connection string key with -q option");
    sys.exit(2);