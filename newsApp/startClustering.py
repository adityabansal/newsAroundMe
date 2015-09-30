import time

from constants import *
from clusterManager import ClusterManager
from distanceTableManager import DistanceTableManager
from docManager import DocManager
from loggingHelper import *
from jobManager import JobManager
from shingleTableManager import ShingleTableManager
from workerJob import WorkerJob

InitLogging()

CLUSTERING_DOC_AGE_LIMIT = 1

def adjustThroughputAfterParsing(jobManager, shingleTableManager):
    """
    Reduce write throughput on shingle table after parsing is done
    """

    job = WorkerJob(
        JOB_UPDATEDBTHROUGHPUT,
        {
            JOB_UPDATEDBTHROUGHPUT_CONNECTIONSTRING : shingleTableManager.tableConnString,
            JOB_UPDATEDBTHROUGHPUT_READTHOUGHPUT: 14,
            JOB_UPDATEDBTHROUGHPUT_WRITETHOUGHPUT: 5,
            JOB_UPDATEDBTHROUGHPUT_INDEXNAME: None
        })
    jobManager.enqueueJob(job)
    logging.info(
        "Put job to reduce shingleTable write throughput. jobId: %s",
        job.jobId)

    job = WorkerJob(
        JOB_UPDATEDBTHROUGHPUT,
        {
            JOB_UPDATEDBTHROUGHPUT_CONNECTIONSTRING : shingleTableManager.tableConnString,
            JOB_UPDATEDBTHROUGHPUT_READTHOUGHPUT: 1,
            JOB_UPDATEDBTHROUGHPUT_WRITETHOUGHPUT: 5,
            JOB_UPDATEDBTHROUGHPUT_INDEXNAME: 'docIdIndex'
        })
    jobManager.enqueueJob(job)
    logging.info(
        "Put job to reduce shingleTable secondary write throughput. jobId: %s",
        job.jobId)

def startClustering():
    """
    Start clustering the docs.
    """

    clusterManager = ClusterManager()
    distanceTableManager = DistanceTableManager()
    docManager = DocManager()
    jobManager = JobManager()
    shingleTableManager = ShingleTableManager()

    shingleTableManager.createFreshTable();
    logging.info("Cleaned up the shingle table");

    distanceTableManager.createFreshTable();
    logging.info("Cleaned up the distance table");

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

    logging.info("Sleeping to ensure that all parse doc jobs are enqueued.")
    time.sleep(10);

    for docKey in docKeys:
        job = WorkerJob(
            JOB_GETCANDIDATEDOCS,
            { JOBARG_GETCANDIDATEDOCS_DOCID : docKey })
        jobManager.enqueueJob(job)
        logging.info(
            "Put get candidate doc job with jobId: %s for docId: %s",
            job.jobId,
            docKey)

    adjustThroughputAfterParsing(jobManager, shingleTableManager)

if __name__ == '__main__':
    startClustering()