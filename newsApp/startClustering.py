import getopt
import os
import sys
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

def putParseDocJobs(jobManager, docKeys):
    for docKey in docKeys:
        parseDocJob = WorkerJob(
            JOB_PARSEDOC,
            { JOBARG_PARSEDOC_DOCID : docKey})
        jobManager.enqueueJob(parseDocJob)
        logging.info(
            "Parse doc job put for docId: %s. Job id: %s",
            docKey,
            parseDocJob.jobId)

def putGetCandidateDocsJobs(jobManager, docKeys):
    for docKey in docKeys:
        job = WorkerJob(
            JOB_GETCANDIDATEDOCS,
            { JOBARG_GETCANDIDATEDOCS_DOCID : docKey })
        jobManager.enqueueJob(job)
        logging.info(
            "Put get candidate doc job with jobId: %s for docId: %s",
            job.jobId,
            docKey)

def putCleanUpDocShinglesJobs(jobManager, docKeys):
    for docKey in docKeys:
        job = WorkerJob(
            JOB_CLEANUPDOCSHINGLES,
            { JOBARG_CLEANUPDOCSHINGLES_DOCID : docKey})
        jobManager.enqueueJob(job)
        logging.info(
            "Put cleanup doc shingles job for docId: %s. Job id: %s",
            docKey,
            job.jobId)

def putCleanUpDocDistancesJobs(jobManager, docKeys):
    for docKey in docKeys:
        job = WorkerJob(
            JOB_CLEANUPDOCDISTANCES,
            { JOBARG_CLEANUPDOCDISTANCES_DOCID : docKey})
        jobManager.enqueueJob(job)
        logging.info(
            "Put cleanup doc distances job for docId: %s. Job id: %s",
            docKey,
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

    putParseDocJobs(jobManager, docKeys);

    logging.info("Sleeping to ensure that all parse doc jobs are enqueued.")
    time.sleep(10);

    putGetCandidateDocsJobs(jobManager, docKeys);

def startIncrementalClustering():
    """
    Start incremental clustering using clusters in previous run of the docs.
    """

    clusterManager = ClusterManager()
    distanceTableManager = DistanceTableManager()
    docManager = DocManager()
    jobManager = JobManager()
    shingleTableManager = ShingleTableManager()

    docKeys = list(docManager.getNewDocKeys(CLUSTERING_DOC_AGE_LIMIT));
    logging.info("Got docs for clustering");

    (newDocs, retainedDocs, expiredDocs) = \
        clusterManager.initNewIncrementalCluster(docKeys)
    logging.info("Initialized new cluster");

    logging.info("Number of docs to cluster are: %i", len(docKeys))
    logging.info(
        "Number of new, retained and expired docs are: %i, %i, %i",
        len(newDocs),
        len(retainedDocs),
        len(expiredDocs))

    putCleanUpDocShinglesJobs(jobManager, expiredDocs)

    putParseDocJobs(jobManager, newDocs)

    putCleanUpDocDistancesJobs(jobManager, expiredDocs)

    logging.info("Sleeping to ensure shingles table is in good state before putting get candidate jobs.")
    time.sleep(10);

    putGetCandidateDocsJobs(jobManager, newDocs);

def isClusteringInProgress():
    clusterManager = ClusterManager()

    if clusterManager.getState() == CLUSTER_STATE_COMPLETED:
        return False;

    logging.info("Clustering currently in progress")
    return True;

if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            opts, args = getopt.getopt(sys.argv[1:], "i", ["incremental"])
        except getopt.GetoptError as err:
            print str(err)
            sys.exit(2)
        for o, a in opts:
            if o in ("-i", "--incremental"):
                if not isClusteringInProgress():
                    startIncrementalClustering()
                sys.exit()

    if not isClusteringInProgress():
        startClustering()
