import itertools
import logging
from multiprocessing import Pool

from retrying import retry

from constants import *
from cluster import Cluster
from clusterManager import ClusterManager
from distanceTableManager import DistanceTableManager
from doc import Doc
from docManager import DocManager
from entityTableManager import EntityTableManager
from clusterJobManager import ClusterJobManager
from minerJobManager import MinerJobManager
from loggingHelper import *
from shingleTableManager import ShingleTableManager
from workerJob import WorkerJob
import textHelper as th

logger = logging.getLogger('clusteringJobs')

# weights. must add up to 1
W_TITLE_SIM = 0.2
W_SUMMARY_SIM = 0.3
W_CONTENT_SIM = 0.5

SIMSCORE_MIN_THRESHOLD = 0.05

def __getDocEnglishTitle(doc):
  docLang = doc.tags[FEEDTAG_LANG]

  if docLang != LANG_ENGLISH:
    return doc.tags[DOCTAG_TRANSLATED_TITLE]
  else:
    return doc.tags[LINKTAG_TITLE]

def __getDocEnglishSummaryText(doc):
  docLang = doc.tags[FEEDTAG_LANG]

  if docLang != LANG_ENGLISH:
    return doc.tags[DOCTAG_TRANSLATED_SUMMARYTEXT]
  else:
    return doc.tags[LINKTAG_SUMMARYTEXT]

def __getDocEnglishContent(doc):
  docLang = doc.tags[FEEDTAG_LANG]

  if docLang != LANG_ENGLISH:
    return doc.tags[DOCTAG_TRANSLATED_CONTENT]
  else:
    return doc.content

def computeEnglishDocsSimScore(doc1, doc2):
    titleSim = th.compareEnglishTitles(
        __getDocEnglishTitle(doc1),
        __getDocEnglishTitle(doc2))

    summarySim = th.compareEnglishTexts(
        __getDocEnglishSummaryText(doc1),
        __getDocEnglishSummaryText(doc2))

    contentSim = th.compareEnglishTexts(
        __getDocEnglishContent(doc1),
        __getDocEnglishContent(doc2))

    return titleSim*W_TITLE_SIM \
           + summarySim*W_SUMMARY_SIM \
           + contentSim*W_CONTENT_SIM;

def computeDocSimScoreUsingEntities(doc1, doc2):
    titleSim = th.compareEnglishTitles(
        __getDocEnglishTitle(doc1),
        __getDocEnglishTitle(doc2))

    titleSimEntities = th.compareTextEntities(
        __getDocEnglishTitle(doc1),
        __getDocEnglishTitle(doc2))

    summarySim = th.compareTextEntities(
        __getDocEnglishSummaryText(doc1),
        __getDocEnglishSummaryText(doc2))

    contentSim = th.compareTextEntities(
        __getDocEnglishContent(doc1),
        __getDocEnglishContent(doc2))

    return titleSim*0.3 \
           + titleSimEntities*0.3 \
           + summarySim*0.2 \
           + contentSim*0.2;

def compareDocs(jobId, doc1Key, doc2Key):
    jobInfo = "Doc1 id: " + doc1Key + " Doc2 id: " + doc2Key \
              + ". Job id: " + jobId;
    logger.info("Started comparing docs. %s", jobInfo);

    docManager = DocManager();
    doc1 = docManager.get(doc1Key)
    doc2 = docManager.get(doc2Key)

    score = 0;
    if (doc1.tags[FEEDTAG_LANG] == LANG_ENGLISH) and \
        (doc2.tags[FEEDTAG_LANG] == LANG_ENGLISH):
        score = computeEnglishDocsSimScore(doc1, doc2)
        logger.info("Comparing using shingles. %s", jobInfo)
    else:
        score = computeDocSimScoreUsingEntities(doc1, doc2)
        logger.info("Comparing using entities. %s", jobInfo)
    logger.info("Comparision score: %s. %s", str(score), jobInfo);

    if score > SIMSCORE_MIN_THRESHOLD:
        distanceTableManager = DistanceTableManager();
        distanceTableManager.addEntry(doc1Key, doc2Key, score);
        logger.info("Added comparision score to distances table. %s", jobInfo);

    logger.info("Completed comparing docs. %s", jobInfo);

@retry(stop_max_attempt_number=3)
def cleanUpDoc(jobId, docId):
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
    logger.info("Started cleaning up doc. %s.", docAndJobId);

    jobManager = ClusterJobManager();

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

def processNewCluster(jobId, strCluster):
    cluster = eval(strCluster)
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
        shingles = th.getStemmedShingles(
          __getDocEnglishSummaryText(doc), 2, 3)
        shingles = shingles + th.getStemmedShingles(
          __getDocEnglishContent(doc), 3, 3)
        logger.info("Completed getting shingles. %s.", docAndJobId)
        logger.info(
          "Number of unique shigles are %i. %s.",
          len(set(shingles)),
          docAndJobId)

        shingleTableManager = ShingleTableManager()
        shingleTableManager.addEntries(docId, shingles);
        logger.info("Added shingles to shingle table. %s.", docAndJobId)

    # compute and put entities
    entities = th.getEntities(__getDocEnglishTitle(doc)) + \
        th.getEntities(__getDocEnglishSummaryText(doc));
    entities = list(set(entities))
    logger.info("Completed getting entities. %s.", docAndJobId)
    logger.info(
      "Number of unique entities are %i. %s.",
      len(set(entities)),
      docAndJobId)

    entityTableManager = EntityTableManager()
    entityTableManager.addEntries(docId, entities)
    logger.info("Added entities to entity table. %s.", docAndJobId)

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
    return matches;

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

    matches = [match for match in matchFreq.keys() if matchFreq[match] > 1]

    logger.info("%i matching docs found using entities. %s.", len(matches), docAndJobId)
    return matches

def putComareDocJobs(docId, matches, docAndJobId):
    jobManager = ClusterJobManager()

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
    docAndJobId = "Doc id: " + docId + ". Job id: " + jobId;
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

## Agglomerative clustering logic ##
MIN_CLUSTER_SIMILARITY = 0.15

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
