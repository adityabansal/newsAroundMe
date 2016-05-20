import getopt
import os
import sys
import time

from constants import *
from clusterManager import ClusterManager
from clusterTableManager import ClusterTableManager
from distanceTableManager import DistanceTableManager
from entityTableManager import EntityTableManager
from docManager import DocManager
from loggingHelper import *
from clusterJobManager import ClusterJobManager
from shingleTableManager import ShingleTableManager
from workerJob import WorkerJob

InitLogging()

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

def putCleanUpDocJobs(jobManager, docKeys):
    for docKey in docKeys:
        job = WorkerJob(JOB_CLEANUPDOC, { JOBARG_CLEANUPDOC_DOCID : docKey})
        jobManager.enqueueJob(job)
        logging.info(
            "Put cleanup doc job for docId: %s. Job id: %s",
            docKey,
            job.jobId)

def startClustering():
    """
    Start clustering the docs.
    """

    clusterManager = ClusterManager()
    clusterTableManager = ClusterTableManager()
    distanceTableManager = DistanceTableManager()
    docManager = DocManager()
    entityTableManager = EntityTableManager()
    jobManager = ClusterJobManager()
    shingleTableManager = ShingleTableManager()

    shingleTableManager.createFreshTable();
    logging.info("Cleaned up the shingle table");

    entityTableManager.createFreshTable();
    logging.info("Cleaned up the entity table");

    distanceTableManager.createFreshTable();
    logging.info("Cleaned up the distance table");

    clusterTableManager.createFreshTable();
    logging.info("Cleaned up the cluster table")

    docKeys = list(docManager.getNewDocKeys(CLUSTERING_DOC_AGE_LIMIT));
    logging.info("Got docs for clustering");

    clusterManager.initNewClusters(docKeys);
    logging.info("Initialized new cluster");
    logging.info("Number of docs to cluster are: %i", len(docKeys))

    putGetCandidateDocsJobs(jobManager, docKeys);
    clusterManager.setState(CLUSTER_STATE_NEW)

def startIncrementalClustering():
    """
    Start incremental clustering using clusters in previous run of the docs.
    """

    clusterManager = ClusterManager()
    docManager = DocManager()
    jobManager = ClusterJobManager()

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

    putCleanUpDocJobs(jobManager, expiredDocs)

    logging.info("Sleeping to ensure shingles table is in good state before putting get candidate jobs.")
    time.sleep(30);

    putGetCandidateDocsJobs(jobManager, newDocs);
    clusterManager.setState(CLUSTER_STATE_NEW)

def isClusteringInProgress():
    clusterManager = ClusterManager()

    if clusterManager.getState() == CLUSTER_STATE_COMPLETED:
        return False;

    logging.info("Clustering currently in progress")
    return True;

if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            opts, args = getopt.getopt(sys.argv[1:], "if", ["incremental"])
        except getopt.GetoptError as err:
            print str(err)
            sys.exit(2)
        for o, a in opts:
            if o in ("-i", "--incremental"):
                if not isClusteringInProgress():
                    startIncrementalClustering()
                sys.exit()
            if o in ["-f"]:
                startClustering()
                sys.exit()

    print("Unsupported option")
    sys.exit(2)
