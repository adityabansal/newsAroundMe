import json
import logging
import random

from .constants import *
from .doc import Doc
from .docManager import DocManager
from .link import Link
from .linkManager import LinkManager
from .minerJobManager import MinerJobManager
from .clusterJobManager import ClusterJobManager
from .cluster import Cluster
from . import htmlProcessor as hp
from . import textHelperNltk as th
from .publisher import Publisher
from .publisherManager import PublisherManager
from .translation import translate
from .workerJob import WorkerJob

logger = logging.getLogger('linkProcessor')

def _generateRandomDocKey():
    return ''.join(random.choice('0123456789ABCDEF') for i in range(16))

def _getDocKey(link):
    # overwrite the existing doc if this link has already been processed.
    if LINKTAG_DOCKEY in link.tags:
        return link.tags[LINKTAG_DOCKEY]
    else:
        return _generateRandomDocKey()

def _getPublisherDetails(publisher):
    return {
        PUBLISHER_DETAILS_FRIENDLYID: publisher.tags[PUBLISHERTAG_FRIENDLYID],
        PUBLISHER_DETAILS_NAME: publisher.tags[PUBLISHERTAG_NAME],
        PUBLISHER_DETAILS_HOMEPAGE: publisher.tags[PUBLISHERTAG_HOMEPAGE]}

def _processHtmlForLink(jobId, link, publisher):
  linkAndJobId = "Link id: " + link.id + ". Job id: " + jobId
  linkId = link.id

  text = ''
  images = []

  pageHtmlUsed = ''
  pageStaticHtml = link.getHtmlStatic()
  logger.info("Got static html for the link. %s.", linkAndJobId)
  resultStatic = hp.processHtml(
    jobId,
    pageStaticHtml,
    publisher.tags[PUBLISHERTAG_TEXTSELECTOR],
    json.loads(publisher.tags[PUBLISHERTAG_IMAGESELECTORS]),
    linkId)

  if len(resultStatic[0]) > 500 and len(resultStatic[1]) > 0:
    logger.info(
      "Text and images extracted using static html. %s.",
      linkAndJobId)
    text = resultStatic[0]
    images = resultStatic[1]
    pageHtmlUsed = pageStaticHtml
  else:
    pageDynamicHtml = link.getHtmlDynamic()
    logger.info("Got dynamic html for the link. %s.", linkAndJobId)
    resultDynamic = hp.processHtml(
      jobId,
      pageDynamicHtml,
      publisher.tags[PUBLISHERTAG_TEXTSELECTOR],
      json.loads(publisher.tags[PUBLISHERTAG_IMAGESELECTORS]),
      linkId)

    text = resultDynamic[0]
    if len(text) < len(resultStatic[0]):
      text = resultStatic[0]
    images = list(set(resultStatic[1] + resultDynamic[1]))
    pageHtmlUsed = pageDynamicHtml

  ogData = hp.extractOpenGraphData(jobId, pageHtmlUsed, linkId)
  if len(images) == 0:
    images = ogData['images']
    logger.info("Using %i images from Open Graph metadata", len(images))

  return (text, images, ogData['summary'])

def _addTranslationTags(jobId, doc):
  docLang = doc.tags[FEEDTAG_LANG]

  if docLang != LANG_ENGLISH:
    doc.tags[DOCTAG_TRANSLATED_TITLE] = translate(
      jobId,
      doc.tags[LINKTAG_TITLE],
      docLang)
    if LINKTAG_SUMMARYTEXT in doc.tags:
      doc.tags[DOCTAG_TRANSLATED_SUMMARYTEXT] = translate(
        jobId,
        doc.tags[LINKTAG_SUMMARYTEXT],
        docLang)
    doc.tags[DOCTAG_TRANSLATED_CONTENT] = translate(
      jobId,
      doc.content,
      docLang)

  return doc

def _addSummaryIfNotPresent(doc, ogSummary):
  if LINKTAG_SUMMARYTEXT not in doc.tags:
    if ogSummary != "":
      doc.tags[LINKTAG_SUMMARYTEXT] = ogSummary
    else:
      doc.tags[LINKTAG_SUMMARYTEXT] = doc.content[:200]
  return doc

PUBLISHERS_BLACKLISTED_FOR_HIGHLIGHTS = []
def _getDocHighlights(doc):
  if doc.tags[FEEDTAG_LANG] == LANG_ENGLISH:
    if doc.tags[TAG_PUBLISHER] not in PUBLISHERS_BLACKLISTED_FOR_HIGHLIGHTS:
      return th.getImportantSentences(doc.content)

  return []

def processLink(jobId, linkId):
  """
  Processes a link(takes as input the linkId)

  Steps:
  1. get link from database
  2. get publisher for that link from database
  3. get html for that link
  4. process that html to generate doc
  5. save that doc in docstore.
  6. update the link's is processed tag.
  """

  linkAndJobId = "Link id: " + linkId + ". Job id: " + jobId
  logger.info("Started processing link. %s.", linkAndJobId)
  
  # get the link
  linkManager = LinkManager()
  link = linkManager.get(linkId)
  logger.info("Got link from database. %s.", linkAndJobId)

  # get the publisher
  publisherManager = PublisherManager()
  publisher = publisherManager.get(link.tags[TAG_PUBLISHER])
  logger.info(
    "Got publisher from database. Publisher id: %s. %s.",
    link.tags[TAG_PUBLISHER],
    linkAndJobId)

  # get html for the link
  processingResult = _processHtmlForLink(jobId, link, publisher)
  if not processingResult[0]:
    logger.warning("No text extracted for the link. %s.", linkAndJobId)

  # generate corresponding doc
  doc = Doc(_getDocKey(link), processingResult[0], link.tags)
  doc.tags[TAG_IMAGES] = processingResult[1]
  doc = _addSummaryIfNotPresent(doc, processingResult[2])
  doc.tags[DOCTAG_URL] = linkId
  doc.tags[TAG_PUBLISHER_DETAILS] = _getPublisherDetails(publisher)
  doc = _addTranslationTags(jobId, doc)
  doc.tags[LINKTAG_HIGHLIGHTS] = _getDocHighlights(doc)

  # save the doc
  docManager = DocManager()
  docManager.put(doc)
  logger.info(
      "Document generated and saved for link. Doc key %s. %s.",
      doc.key,
      linkAndJobId)

  #update the doc key in links table
  link.tags[LINKTAG_DOCKEY] = doc.key
  linkManager.put(link)

  # put parse doc job
  parseDocJob = WorkerJob(JOB_PARSEDOC, { JOBARG_PARSEDOC_DOCID : doc.key})
  jobManager = MinerJobManager()
  jobManager.enqueueJob(parseDocJob)
  logger.info(
    "Parse doc job with with jobId '%s' put. %s.",
    parseDocJob.jobId,
    linkAndJobId)

  isLinkNotProcessed = (link.tags[LINKTAG_ISPROCESSED] != 'true')
  isClusteringAllowed = (FEEDTAG_DO_NOT_CLUSTER not in doc.tags)
  if isLinkNotProcessed and isClusteringAllowed:
    newCluster = Cluster([doc.key])
    processNewClusterJob = WorkerJob(
      JOB_PROCESSNEWCLUSTER,
      { JOBARG_PROCESSNEWCLUSTER_CLUSTER : list(newCluster)})
    clusterJobManager = ClusterJobManager()
    clusterJobManager.enqueueJob(processNewClusterJob)
    logging.info(
      "Put process new cluster job for new doc. Cluster id: %s. %s",
      newCluster.id,
      linkAndJobId)

  # update the link
  link.tags[LINKTAG_ISPROCESSED] = 'true'
  linkManager.put(link)
  logger.info(
    "Link updated after being successfully processed. %s.",
    linkAndJobId)

  logger.info("Completed processing link. %s.", linkAndJobId)
