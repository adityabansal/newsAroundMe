from linkManager import LinkManager
from workerJob import WorkerJob
from minerJobManager import MinerJobManager
from constants import *
from loggingHelper import *

InitLogging()

def pushLinkJobs():
    """
    Cleanup old links in the links table.
    Run this job periodically.
    """

    jobManager = MinerJobManager()
    linkManager = LinkManager()

    logging.info("Getting unprocessed links.")
    links = linkManager.getUnprocessedLinks()

    nLinks = 0;
    for linkId in links:
        processLinkJob = WorkerJob(
            JOB_PROCESSLINK,
            { JOBARG_PROCESSLINK_LINKID : linkId})
        jobManager.enqueueJob(processLinkJob)
        logging.info(
            "Process link job with jobId '%s' put for linkId: %s.",
            processLinkJob.jobId,
            linkId)
        nLinks = nLinks + 1

    logging.info("Number of process link jobs  were: %i", nLinks)

if __name__ == '__main__':
    pushLinkJobs();