import logging

from constants import *
from clusterManager import ClusterManager
from doc import Doc
from docManager import DocManager
from loggingHelper import *
from shingleTableManager import ShingleTableManager
import textHelper as th

logger = logging.getLogger('clusteringJobs')

# weights. must add up to 1
W_TITLE_SIM = 0.2
W_SUMMARY_SIM = 0.4
W_CONTENT_SIM = 0.4

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

def compareDocs(doc1Key, doc2Key):
    docManager = DocManager();

    doc1 = docManager.get(doc1Key)
    doc2 = docManager.get(doc2Key)

    return computeDocSimScore(doc1, doc2);

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

def getCandidateDocs(jobId):
    jobInfo = "Job id: " + jobId
    logger.info("Computing candidate docs. %s.", jobInfo)

    shingleTableManager = ShingleTableManager()
    clusterManager = ClusterManager()

    docList = clusterManager.getDocList()
    logger.info("Got document list. %s.", jobInfo)

    for docKey in docList:
        logger.info("Finding matches for docId: %s. %s.", docKey, jobInfo)

        matches = set()

        shingles = shingleTableManager.queryByDocId(docKey)
        for shingle in shingles:
            matchingDocs = shingleTableManager.queryByShingle(shingle)
            for match in matchingDocs:
                if (match > docKey):
                    logger.info(
                        "Candidate docs %s and %s found. Shingle: %s. %s",
                        docKey,
                        match,
                        shingle,
                        jobInfo)
                    matches.add(match)

        logger.info("Total %i matching docs for docId: %s. %s.", len(matches), docKey, jobInfo)
