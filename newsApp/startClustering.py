from constants import *
from docManager import DocManager
from loggingHelper import *
from jobManager import JobManager
from shingleTableManager import ShingleTableManager
from workerJob import WorkerJob

InitLogging()

CLUSTERING_DOC_AGE_LIMIT = 1

def startClustering():
    """
    Start clustering the docs.
    """

    docManager = DocManager()
    jobManager = JobManager()
    shingleTableManager = ShingleTableManager()

    logging.info("Cleaning up the shingle table")
    shingleTableManager.createFreshTable();

    logging.info("Getting docs for clustering")
    docKeys = docManager.getNewDocKeys(CLUSTERING_DOC_AGE_LIMIT);

    nStaleDocs = 0;
    for docKey in docKeys:
        parseDocJob = WorkerJob(
            JOB_PARSEDOC,
            { JOBARG_PARSEDOC_DOCID : docKey})
        jobManager.enqueueJob(parseDocJob)
        logging.info(
            "Parse doc job put for docId: %s. Job id: %s",
            docKey,
            parseDocJob.jobId)
        nStaleDocs = nStaleDocs + 1;

    logging.info("Number of docs to cluster are: %i", nStaleDocs)

if __name__ == '__main__':
    startClustering()
