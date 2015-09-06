import itertools
import logging

from constants import *
from clusterManager import ClusterManager
from distanceTableManager import DistanceTableManager
from doc import Doc
from docManager import DocManager
from jobManager import JobManager
from loggingHelper import *
from shingleTableManager import ShingleTableManager
from workerJob import WorkerJob
import textHelper as th

logger = logging.getLogger('clusteringJobs')

# weights. must add up to 1
W_TITLE_SIM = 0.2
W_SUMMARY_SIM = 0.3
W_CONTENT_SIM = 0.5

SIMSCORE_MIN_THRESHOLD = 0.02

def computeDocSimScore(doc1, doc2):
    titleSim = th.compareTitles(
        doc1.tags[LINKTAG_TITLE],
        doc2.tags[LINKTAG_TITLE])

    summarySim = th.compareTexts(
        doc1.tags[LINKTAG_SUMMARYTEXT],
        doc2.tags[LINKTAG_SUMMARYTEXT])

    contentSim = th.compareTexts(doc1.content, doc2.content)

    return titleSim*W_TITLE_SIM \
           + summarySim*W_SUMMARY_SIM \
           + contentSim*W_CONTENT_SIM;

def compareDocs(jobId, doc1Key, doc2Key):
    jobInfo = "Doc1 id: " + doc1Key + "Doc2 id: " + doc2Key \
              + ". Job id: " + jobId;
    logger.info("Started comparing docs. %s", jobInfo);

    docManager = DocManager();
    distanceTableManager = DistanceTableManager();

    doc1 = docManager.get(doc1Key)
    doc2 = docManager.get(doc2Key)
    score = computeDocSimScore(doc1, doc2);
    logger.info("Comparision score: %s. %s", str(score), jobInfo);

    if score > SIMSCORE_MIN_THRESHOLD:
        distanceTableManager.addEntry(doc1Key, doc2Key, score);
        logger.info("Added comparision score to distances table. %s", jobInfo);

    logger.info("Completed comparing docs. %s", jobInfo);

def parseDoc(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started parsing doc. %s.", docAndJobId)

    docManager = DocManager()
    shingleTableManager = ShingleTableManager()

    doc = docManager.get(docId)
    shingles = th.getStemmedShingles(doc.tags[LINKTAG_SUMMARYTEXT], 2, 3)
    shingles = shingles + th.getStemmedShingles(doc.content, 3, 3)
    logger.info("Completed getting shingles. %s.", docAndJobId)

    shingleTableManager.addEntries(docId, shingles);

    logger.info("Completed parsing doc. %s.", docAndJobId)

def getCandidateDocs(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started get candidate docs job. %s.", docAndJobId)

    matches = set()
    shingleTableManager = ShingleTableManager()
    jobManager = JobManager()

    shingles = shingleTableManager.queryByDocId(docId)
    for shingle in shingles:
         matchingDocs = shingleTableManager.queryByShingle(shingle)
         for match in matchingDocs:
             matches.add(match)

    logger.info("%i matching docs found. %s.", len(matches), docAndJobId)
    for match in matches:
        if (match > docId):
            job = WorkerJob(
                JOB_COMPAREDOCS,
                {
                    JOBARG_COMPAREDOCS_DOC1ID : docId,
                    JOBARG_COMPAREDOCS_DOC2ID : match
                })
            jobManager.enqueueJob(job)
            logging.info(
                "Put compare docs job with jobid: %s. compared docId: %s. %s",
                job.jobId,
                match,
                docAndJobId)

    logger.info("Completed get candidate docs job. %s.", docAndJobId)

## Agglomerative clustering logic ##
MIN_CLUSTER_SIMILARITY = 0.1

def _getDocDistance(distances, docId1, docId2):
    first = min(docId1, docId2)
    second = max(docId1, docId2)

    try:
        match = next(d for d in distances
                     if (d[0] == first) & (d[1] == second))
        return match[2]
    except StopIteration:
        return 0

def _getClusterDistance(distances, cluster1, cluster2):
    dPairs = [_getDocDistance(distances, x[0], x[1])
                 for x in itertools.product(cluster1, cluster2)]

    return sum(dPairs)/len(dPairs)

def _getClosestClusters(clusters, distances):
    maxSimilarity = 0
    result = None

    for (c1, c2) in itertools.combinations(clusters, 2):
        d = _getClusterDistance(distances, c1, c2)
        if d >= maxSimilarity:
            maxSimilarity = d
            result = (c1, c2, d)

    return result

def _mergeClusters(clusters, cluster1, cluster2):
    mergedCluster = cluster1 | cluster2
    clusters.remove(cluster1)
    clusters.remove(cluster2)
    clusters.append(mergedCluster)

    return clusters

def clusterDocs(jobId):
    jobInfo = "Job id: " + jobId
    logger.info("Started clustering docs. %s.", jobInfo)

    distanceTableManager = DistanceTableManager()
    clusterManager = ClusterManager()

    distances = list(distanceTableManager.getEntries())
    logger.info("Got the pairwise distances. %s.", jobInfo)

    clusters = clusterManager.getClusters()
    logger.info("Got the clusters. %s.", jobInfo)

    logger.info("Started clustering. %s.", jobInfo)
    while True:
        (cluster1, cluster2, similarity) = _getClosestClusters(
            clusters,
            distances)
        if similarity > MIN_CLUSTER_SIMILARITY:
            clusters = _mergeClusters(clusters, cluster1, cluster2)
            logger.info(
                "Merged clusters %s and %s with score %s. %s",
                 cluster1,
                 cluster2,
                 similarity,
                 jobInfo)
        else:
            break;
    logger.info("Finished clustering. %s.", jobInfo)

    clusterManager.putClusters(clusters)
    logger.info("Put the computed clusters. %s.", jobInfo)
