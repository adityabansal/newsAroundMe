from constants import *
from loggingHelper import *
from feedManager import FeedManager
from workerJob import WorkerJob
from minerJobManager import MinerJobManager

InitLogging()

def pushFeedJobs():
    """
    Push feed processing jobs to job queue.
    """

    jobManager = MinerJobManager()
    feedManager = FeedManager()
    
    logging.info("Getting stale  feeds.")
    staleFeeds = feedManager.getStaleFeeds();

    nStaleFeeds = 0;
    for feed in staleFeeds:
        processFeedJob = WorkerJob(
            JOB_PROCESSFEED,
            { JOBARG_PROCESSFEED_FEEDID : feed})
        jobManager.enqueueJob(processFeedJob)
        logging.info(
            "Process feed job put for feedId: %s. Job id: %s",
            feed,
            processFeedJob.jobId)
        nStaleFeeds = nStaleFeeds + 1;

    logging.info("Number of stale feeds are: %i", nStaleFeeds)

if __name__ == '__main__':
    pushFeedJobs()
