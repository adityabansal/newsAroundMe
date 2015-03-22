from constants import *
from loggingHelper import *
from feedManager import FeedManager
from workerJob import WorkerJob
from jobManager import JobManager

InitLogging()

def pushFeedJobs():
    """
    Push feed processing jobs to job queue.
    """

    jobManager = JobManager()
    feedManager = FeedManager()
    
    logging.info("Getting stale  feeds.")
    staleFeeds = feedManager.getStaleFeeds()
    logging.info("Number of stale feeds are: %i", len(staleFeeds))

    for feed in staleFeeds:
        processFeedJob = WorkerJob(
            JOB_PROCESSFEED,
            { JOBARG_PROCESSFEED_FEEDID : feed})
        jobManager.enqueueJob(processFeedJob)
        logging.info("Process feed job put for feedId: %s", feed)

if __name__ == '__main__':
    pushFeedJobs()
