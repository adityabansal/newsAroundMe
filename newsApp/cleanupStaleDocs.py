from constants import *
from docManager import DocManager
from loggingHelper import *
from minerJobManager import MinerJobManager
from workerJob import WorkerJob

InitLogging()

def cleanupStaleDocs():
    """
    Cleanup old docs.
    Run this job periodically.
    """

    docManager = DocManager()
    jobManager = MinerJobManager()
    
    logging.info("Getting stale docs.")
    staleDocKeys = docManager.getStaleDocKeys()

    for docKey in staleDocKeys:
        job = WorkerJob(JOB_CLEANUPDOC, { JOBARG_CLEANUPDOC_DOCID : docKey})
        jobManager.enqueueJob(job)
        logging.info(
            "Put cleanup doc job for docId: %s. Job id: %s",
            docKey,
            job.jobId)
        logging.info("Deleting doc with key: %s", docKey);
        docManager.delete(docKey);
        logging.info("Deleted doc with key: %s", docKey);

    logging.info("Number of stale docs deleted were: %i", len(list(staleDocKeys))

if __name__ == '__main__':
    cleanupStaleDocs();
