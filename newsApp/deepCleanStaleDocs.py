import time
from boto.exception import S3ResponseError

from .constants import *
from .docManager import DocManager
from .loggingHelper import *
from .minerJobManager import MinerJobManager
from .workerJob import WorkerJob
from .shingleTableManager import ShingleTableManager
from .clusterManager import ClusterManager

InitLogging()

def putCleanupDocJob(docId):
    jobManager = MinerJobManager()

    job = WorkerJob(JOB_CLEANUPDOC, { JOBARG_CLEANUPDOC_DOCID : docId})
    jobManager.enqueueJob(job)
    logging.info(
        "Put cleanup doc job for docId: %s. Job id: %s",
        docId,
        job.jobId)

def deepCleanStaleDocs():
    """
    Puts cleanup doc jobs for stale entries in shingles table
    Run this job rarely.
    """

    docManager = DocManager()
    shingleTableManager = ShingleTableManager()
    clusterManager = ClusterManager()

    validDocs = list(clusterManager.getCurrentDocs())
    docsToBeCleanedUp = []
    
    logging.info("Started scanning the shingle table")
    scanResults = shingleTableManager.scan()

    for entry in scanResults:
        docId = entry[0]
        shingle = entry[1]

        # check if the doc exists.
        try:
            doc = docManager.get(docId)
        except S3ResponseError:
            logging.info("Non existent doc shingle found -> docId: %s, shingle: %s", docId, shingle)
            if docId not in docsToBeCleanedUp:
                docsToBeCleanedUp.append(docId)
                putCleanupDocJob(docId)

        # If it's in current docs or has already been checked, move onto next entry
        if docId in validDocs:
            continue

        # Check the doc's age.
        if doc.tags[LINKTAG_PUBTIME] < (int(time.time()) - CLUSTERING_DOC_AGE_LIMIT * 60 * 60 * 24):
            logging.info("Stale doc shingle found -> docId: %s, shingle: %s", docId, shingle)
            if docId not in docsToBeCleanedUp:
                docsToBeCleanedUp.append(docId)
                putCleanupDocJob(docId)
        else:
            validDocs.append(docId) # doc is new (added to current clusters b/w here and L25)

    logging.info("Number of stale docs deleted were: %i", len(list(docsToBeCleanedUp)))

if __name__ == '__main__':
    deepCleanStaleDocs()