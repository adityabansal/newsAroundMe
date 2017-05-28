from boto.exception import S3ResponseError

from constants import *
from docManager import DocManager
from loggingHelper import *
from minerJobManager import MinerJobManager
from workerJob import WorkerJob
from shingleTableManager import ShingleTableManager

InitLogging()

def deepCleanStaleDocs():
    """
    Puts cleanup doc jobs for stale entries in shingles table
    Run this job rarely.
    """

    docManager = DocManager()
    jobManager = MinerJobManager()
    shingleTableManager = ShingleTableManager()
    docsToBeCleanedUp = []
    
    logging.info("Started scanning the shingle table")
    scanResults = shingleTableManager.scan()

    for entry in scanResults:
        try:
            docManager.get(entry[0])
        except S3ResponseError:
            staleDocId = entry[0];
            staleShingle = entry[1];
            logging.info("Stale entry found -> docId: %s, shingle: %s", staleDocId, staleShingle)
            
            if staleDocId not in docsToBeCleanedUp:
                docsToBeCleanedUp.append(staleDocId);
                job = WorkerJob(JOB_CLEANUPDOC, { JOBARG_CLEANUPDOC_DOCID : staleDocId})
                jobManager.enqueueJob(job)
                logging.info(
                    "Put cleanup doc job for docId: %s. Job id: %s",
                    staleDocId,
                    job.jobId)

    logging.info("Number of stale docs deleted were: %i", len(list(docsToBeCleanedUp)))

if __name__ == '__main__':
    deepCleanStaleDocs();