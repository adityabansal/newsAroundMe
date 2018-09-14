import itertools
import logging
import json

from retrying import retry

from constants import *
from cluster import Cluster
from clusterManager import ClusterManager
from distanceTableManager import DistanceTableManager
from doc import Doc
from docHelper import getDocEnglishTitle, getDocEnglishSummaryText, \
  getDocEnglishContent, getDocComparisionScore
from docManager import DocManager
from entityTableManager import EntityTableManager
from minerJobManager import MinerJobManager
from loggingHelper import *
from shingleTableManager import ShingleTableManager
from workerJob import WorkerJob
import textHelperNltk as th

logger = logging.getLogger('clusteringJobs')

SIMSCORE_MIN_THRESHOLD = 0.05

def compareDocs(jobId, doc1Key, doc2Key):
    jobInfo = "Doc1 id: " + doc1Key + " Doc2 id: " + doc2Key \
              + ". Job id: " + jobId
    logger.info("Started comparing docs. %s", jobInfo)

    docManager = DocManager()
    doc1 = docManager.get(doc1Key)
    doc2 = docManager.get(doc2Key)

    score = getDocComparisionScore(jobInfo, doc1, doc2)

    if score > SIMSCORE_MIN_THRESHOLD:
        distanceTableManager = DistanceTableManager()
        distanceTableManager.addEntry(doc1Key, doc2Key, score)
        logger.info("Added comparision score to distances table. %s", jobInfo)

    logger.info("Completed comparing docs. %s", jobInfo)

@retry(stop_max_attempt_number=3)
def cleanUpDoc(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started cleaning up doc. %s.", docAndJobId);

    jobManager = MinerJobManager();

    job = WorkerJob(
        JOB_CLEANUPDOCSHINGLES,
        { JOBARG_CLEANUPDOCSHINGLES_DOCID : docId})
    jobManager.enqueueJob(job)
    logging.info("Put cleanup doc shingles job. %s.", docAndJobId)

    job = WorkerJob(
        JOB_CLEANUPDOCENTITIES,
        { JOBARG_CLEANUPDOCENTITIES_DOCID : docId})
    jobManager.enqueueJob(job)
    logging.info("Put cleanup doc entities job. %s.", docAndJobId)

    job = WorkerJob(
        JOB_CLEANUPDOCDISTANCES,
        { JOBARG_CLEANUPDOCDISTANCES_DOCID : docId})
    jobManager.enqueueJob(job)
    logging.info("Put cleanup doc distances job. %s.", docAndJobId)

@retry(stop_max_attempt_number=3)
def cleanUpDocShingles(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started cleaning up doc shingles. %s.", docAndJobId)

    shingleTableManager = ShingleTableManager()
    shingleTableManager.cleanUpDocShingles(docId)

    logger.info("Completed cleaning up doc shingles. %s.", docAndJobId)

@retry(stop_max_attempt_number=3)
def cleanUpDocEntities(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started cleaning up doc entities. %s.", docAndJobId)

    entityTableManager = EntityTableManager()
    entityTableManager.cleanUpDocEntities(docId)

    logger.info("Completed cleaning up doc entities. %s.", docAndJobId)

@retry(stop_max_attempt_number=3)
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
    jobManager = MinerJobManager()

    docList = list(clusterManager.getCurrentDocs())
    distances = list(distanceTableManager.getEntries())

    staleDocs = []
    for entry in distances:
        staleDoc = ""
        if entry[0] not in docList:
            staleDocs.append(entry[0])
        elif entry[1] not in docList:
            staleDocs.append(entry[1])
    staleDocs = list(set(staleDocs))

    for docKey in staleDocs:
        job = WorkerJob(JOB_CLEANUPDOC, { JOBARG_CLEANUPDOC_DOCID : docKey})
        jobManager.enqueueJob(job)
        logging.info(
            "Put cleanup doc job with id %s for docId: %s. %s",
            job.jobId,
            docKey,
            jobInfo)

    logging.info(
        "Number of stale entries in distances table: %i. %s",
        len(staleDocs),
        jobInfo)

def processNewCluster(jobId, clusterDocs):
    cluster = Cluster(clusterDocs)
    clusterAndJobId = "Cluster id: " + cluster.id + ". Job id: " + jobId;
    logger.info("Started processing new cluster. %s.", clusterAndJobId)

    clusterManager = ClusterManager()
    clusterManager.processNewCluster(cluster)

    logger.info("Completed processing new cluster. %s.", clusterAndJobId)

@retry(stop_max_attempt_number=3)
def parseDoc(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started parsing doc. %s.", docAndJobId)

    docManager = DocManager()
    doc = docManager.get(docId)

    # compute and put shingles
    if (doc.tags[FEEDTAG_LANG] == LANG_ENGLISH):
        shingles = th.getShingles(getDocEnglishSummaryText(doc), 3, 3)
        shingles = shingles + th.getShingles(
          getDocEnglishContent(doc), 3, 3)
        logger.info("Completed getting shingles. %s.", docAndJobId)
        shingles = list(set(shingles))
        logger.info(
          "Number of unique shingles are %i. %s.",
          len(shingles),
          docAndJobId)

        shingleTableManager = ShingleTableManager()
        shingleTableManager.addEntries(docId, shingles)
        logger.info("Added shingles to shingle table. %s.", docAndJobId)

    # compute and put entities
    entities = th.getEntities(getDocEnglishTitle(doc)) + \
        th.getEntities(getDocEnglishSummaryText(doc)) + \
        th.getEntities(getDocEnglishContent(doc))
    entities = list(set(entities))
    logger.info("Completed getting entities. %s.", docAndJobId)
    logger.info(
      "Number of unique entities are %i. %s.",
      len(entities),
      docAndJobId)

    entityTableManager = EntityTableManager()
    entityTableManager.addEntries(docId, entities)
    logger.info("Added entities to entity table. %s.", docAndJobId)

    #store entity weights in the doc
    entityWeights = {}
    for entity in entities:
        entityWeight = entityTableManager.getEntityWeight(entity)
        entityWeights[entity] = entityWeight
    doc.tags[DOCTAG_ENTITY_WEIGHTS] = json.dumps(entityWeights)
    docManager.put(doc)
    logger.info("Added entity weights to doc. %s.", docAndJobId)

    job = WorkerJob(
        JOB_GETCANDIDATEDOCS,
        { JOBARG_GETCANDIDATEDOCS_DOCID : docId })
    jobManager = MinerJobManager()
    jobManager.enqueueJob(job)
    logging.info(
        "Put get candidate doc job with jobId: %s. %s",
        job.jobId,
        docAndJobId)

    logger.info("Completed parsing doc. %s.", docAndJobId)

def getCandidateDocsUsingShingles(jobId, docId, docAndJobId):
    matchFreq = {}
    shingleTableManager = ShingleTableManager()

    shingles = list(shingleTableManager.queryByDocId(docId))

    matchingDocs = []
    for shingle in shingles:
      logger.info("Querying docs for shingle %s. %s", shingle, docAndJobId)
      matchingDocs = matchingDocs + list(shingleTableManager.queryByShingle(shingle))

    for match in matchingDocs:
        if match in matchFreq:
            matchFreq[match] = matchFreq[match] + 1
        else:
            matchFreq[match] = 1

    matches = [match for match in matchFreq.keys() if matchFreq[match] > 4]

    logger.info("%i matching docs found using shingles. %s.", len(matches), docAndJobId)
    return matches

def getCandidateDocsUsingEntities(jobId, docId, docAndJobId):
    entityTableManager = EntityTableManager()
    matchFreq = {}

    entities = entityTableManager.queryByDocId(docId)

    for entity in entities:
        matchingDocs = entityTableManager.queryByEntity(entity)
        for match in matchingDocs:
            if match in matchFreq:
                matchFreq[match] = matchFreq[match] + 1
            else:
                matchFreq[match] = 1

    matches = [match for match in matchFreq.keys() if matchFreq[match] > 2]

    logger.info("%i matching docs found using entities. %s.", len(matches), docAndJobId)
    return matches

def putComareDocJobs(docId, matches, docAndJobId):
    jobManager = MinerJobManager()

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

def getCandidateDocs(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId
    logger.info("Started get candidate docs job. %s.", docAndJobId)

    matchesUsingShingles = getCandidateDocsUsingShingles(jobId, docId, docAndJobId)
    matchesUsingEntities = getCandidateDocsUsingEntities(jobId, docId, docAndJobId)

    uniqueMatches = list(set(matchesUsingShingles) | set(matchesUsingEntities))
    logger.info(
        "%i unique matching docs found using shingles and entities. %s.",
        len(uniqueMatches),
        docAndJobId)
    putComareDocJobs(docId, uniqueMatches, docAndJobId)

    logger.info("Completed get candidate docs job. %s.", docAndJobId)

def getCandidateDocsThroughClusters(jobId):
    jobInfo = "Job id: " + jobId
    distanceTableManager = DistanceTableManager()
    clusterManager = ClusterManager()
    jobManager = MinerJobManager()

    distances = distanceTableManager.getDistanceMatrix()
    logger.info("Got the distance matrix. %s.", jobInfo)

    clusters = list(clusterManager.getCurrentClusters())
    logger.info("Got the clusters. %s.", jobInfo)

    for cluster in clusters:
        if len(cluster) > 1:
            closeDocs = []
            for doc in cluster:
                closeDocs = closeDocs + distanceTableManager.getCloseDocs(doc)
            closeDocs = list(set(closeDocs))

            for (doc1, doc2) in itertools.product(cluster, closeDocs):
                try:
                    _tryGetDocDistance(distances, doc1, doc2)
                    logging.info(
                        "Docs %s and %s already compared. %s",
                        doc1,
                        doc2,
                        jobInfo)
                except KeyError:
                    if doc1 != doc2:
                        job = WorkerJob(
                            JOB_COMPAREDOCS,
                            {
                                JOBARG_COMPAREDOCS_DOC1ID : doc1,
                                JOBARG_COMPAREDOCS_DOC2ID : doc2
                            })
                        jobManager.enqueueJob(job)
                        logging.info(
                            "Put compare docs job with jobid: %s. doc1: %s. doc2: %s. %s",
                            job.jobId,
                            doc1,
                            doc2,
                            jobInfo)

## Agglomerative clustering logic ##
MIN_CLUSTER_SIMILARITY = 0.15

def _tryGetDocDistance(distances, docId1, docId2):
    first = min(docId1, docId2)
    second = max(docId1, docId2)

    return distances[first][second]

def _getDocDistance(distances, docId1, docId2):
    try:
        return _tryGetDocDistance(distances, docId1, docId2)
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
    mergedCluster = Cluster(list(cluster1 | cluster2))
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

    distances = distanceTableManager.getDistanceMatrix()
    logger.info("Got the distance matrix. %s.", jobInfo)

    clusters = list(clusterManager.getCurrentClusters())
    logger.info("Got the clusters. %s.", jobInfo)

    logger.info("Started clustering. %s.", jobInfo)
    clusterHierarchical(jobInfo, clusters, distances)
    logger.info("Finished clustering. %s.", jobInfo)

    clusterManager.putCurrentClusters(clusters)
    logger.info("Put the computed clusters. %s.", jobInfo)