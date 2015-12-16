import itertools
import logging
from multiprocessing import Pool

from retrying import retry

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
    jobInfo = "Doc1 id: " + doc1Key + " Doc2 id: " + doc2Key \
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

def cleanUpDocShingles(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started cleaning up doc shingles. %s.", docAndJobId)

    shingleTableManager = ShingleTableManager()
    shingleTableManager.cleanUpDocShingles(docId)

    logger.info("Completed cleaning up doc shingles. %s.", docAndJobId)

def cleanUpDocDistances(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started cleaning up doc distances. %s.", docAndJobId)

    distanceTableManager = DistanceTableManager()
    distanceTableManager.cleanUpDoc(docId)

    logger.info("Completed cleaning up doc distances. %s.", docAndJobId)

def cleanUpDistanceTable(jobId):
    jobInfo = "Job id: " + jobId
    distanceTableManager = DistanceTableManager()
    clusterManager = ClusterManager()

    docList = clusterManager.getDocList()
    distances = list(distanceTableManager.getEntries())

    nStaleEntries = 0
    for entry in distances:
        if entry[0] not in docList or entry[1] not in docList:
            nStaleEntries = nStaleEntries + 1
            distanceTableManager.deleteEntry(entry[0], entry[1])
            logging.info("Deleted stale entry %s. %s", str(entry), jobInfo)

    logging.info(
        "Number of stale entries in distances table: %i. %s",
        nStaleEntries,
        jobInfo)

def cleanupShingleTable(jobId):
    jobInfo = "Job id: " + jobId
    clusterManager = ClusterManager()
    docManager = DocManager()
    shingleTableManager = ShingleTableManager()

    docList = clusterManager.getDocList()
    allDocs = list(docManager.getNewDocKeys(1.5))

    for docKey in allDocs:
        if docKey not in docList:
            docShingles = list(shingleTableManager.queryByDocId(docKey))
            if len(docShingles) > 0:
                logging.info(
                    "Stale entry found in shingle table for docId %s. %s",
                    docKey,
                    jobInfo)
                shingleTableManager.cleanUpDocShingles(docKey)

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

def __getDocShingles(shingle):
    shingleTableManager = ShingleTableManager()
    return list(shingleTableManager.queryByShingle(shingle))

@retry(stop_max_attempt_number=3)
def __queryShingles(shingles):
    pool = Pool(8)
    poolResults = [[]]
    try:
        poolResults = pool.map_async(__getDocShingles, list(shingles)).get(
            timeout=300)
    except:
        logger.warning("Could not get shingles. %s.", docAndJobId)
        raise
    finally:
        pool.terminate()

    return poolResults

def getCandidateDocs(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started get candidate docs job. %s.", docAndJobId)

    matchFreq = {}
    shingleTableManager = ShingleTableManager()
    jobManager = JobManager()

    shingles = shingleTableManager.queryByDocId(docId)
    poolResults = __queryShingles(shingles)

    matchingDocs = [item for results in poolResults for item in results]

    for match in matchingDocs:
        if match in matchFreq:
            matchFreq[match] = matchFreq[match] + 1
        else:
            matchFreq[match] = 1

    matches = [match for match in matchFreq.keys() if matchFreq[match] > 4]

    logger.info("%i matching docs found. %s.", len(matches), docAndJobId)
    for match in matches:
        if match != docId:
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
MIN_CLUSTER_SIMILARITY = 0.08

def _getDocDistance(distances, docId1, docId2):
    first = min(docId1, docId2)
    second = max(docId1, docId2)

    try:
        return distances[first][second]
    except KeyError:
        return 0

def _getClusterDistance(distances, cluster1, cluster2):
    dPairs = [_getDocDistance(distances, x[0], x[1])
                 for x in itertools.product(cluster1, cluster2)]

    return sum(dPairs)/len(dPairs)

def _getCloseClusters(clusters, distances):
    result = []

    for (c1, c2) in itertools.combinations(clusters, 2):
        d = _getClusterDistance(distances, c1, c2)
        if d > MIN_CLUSTER_SIMILARITY:
            #insert the close clusters at appropiate place in list
            #the list should be sorted with decreasing cluster similarity
            nCloser = 0;
            for i in range(0,len(result)):
                currentValue = result[i][2];
                if currentValue > d:
                    nCloser += 1;
                else:
                    break;

            result.insert(nCloser, (c1, c2, d))

    return result

def _mergeClusters(clusters, cluster1, cluster2):
    mergedCluster = cluster1 | cluster2
    clusters.remove(cluster1)
    clusters.remove(cluster2)
    clusters.append(mergedCluster)

    return clusters

def clusterHierarchical(jobInfo, clusters, distances):
    passNumber = 1
    while  True:
        closeClusters = _getCloseClusters(clusters, distances)
        touchedClusters = []

        for (c1, c2, d) in closeClusters:
            if c1 not in touchedClusters and c2 not in touchedClusters:
                touchedClusters.append(c1)
                touchedClusters.append(c2)
                clusters = _mergeClusters(clusters, c1, c2)
                logger.info(
                    "Pass %i. Merged clusters %s and %s with score %s. %s",
                    passNumber,
                    c1,
                    c2,
                    d,
                    jobInfo)

        if len(closeClusters) == 0:
            break;
        passNumber += 1;

def clusterDocs(jobId):
    jobInfo = "Job id: " + jobId
    logger.info("Started clustering docs. %s.", jobInfo)

    distanceTableManager = DistanceTableManager()
    clusterManager = ClusterManager()

    clusterManager.setState(CLUSTER_STATE_STARTED)
    logger.info("Set clustering state as started. %s.", jobInfo)

    distances = distanceTableManager.getDistanceMatrix()
    logger.info("Got the distance matrix. %s.", jobInfo)

    clusters = clusterManager.getClusters()
    logger.info("Got the clusters. %s.", jobInfo)

    logger.info("Started clustering. %s.", jobInfo)
    clusterHierarchical(jobInfo, clusters, distances)
    logger.info("Finished clustering. %s.", jobInfo)

    clusterManager.putClusters(clusters)
    logger.info("Put the computed clusters. %s.", jobInfo)

    clusterManager.setState(CLUSTER_STATE_COMPLETED)
    logger.info("Set clustering state as completed. %s.", jobInfo)

    logger.info("Starting post-clustering cleanup tasks. %s", jobInfo)
    logger.info("Cleaning up distance table. %s.", jobInfo)
    cleanUpDistanceTable(jobId)

    logger.info("Cleaning up shingle table. %s.", jobInfo)
    cleanupShingleTable(jobId)
    logger.info("Completed post-clustering cleanup tasks. %s", jobInfo)
