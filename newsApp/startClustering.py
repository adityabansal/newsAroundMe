from constants import *
from clusterManager import ClusterManager
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

    clusterManager = ClusterManager()
    docManager = DocManager()
    jobManager = JobManager()
    shingleTableManager = ShingleTableManager()

    shingleTableManager.createFreshTable();
    logging.info("Cleaned up the shingle table");

    docKeys = list(docManager.getNewDocKeys(CLUSTERING_DOC_AGE_LIMIT));
    logging.info("Got docs for clustering");

    clusterManager.initNewClusters(docKeys);
    logging.info("Initialized new cluster");
    logging.info("Number of docs to cluster are: %i", len(docKeys))

    for docKey in docKeys:
        parseDocJob = WorkerJob(
            JOB_PARSEDOC,
            { JOBARG_PARSEDOC_DOCID : docKey})
        jobManager.enqueueJob(parseDocJob)
        logging.info(
            "Parse doc job put for docId: %s. Job id: %s",
            docKey,
            parseDocJob.jobId)

if __name__ == '__main__':
    startClustering()
