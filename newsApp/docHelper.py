import json
import logging

from .constants import *
from . import textHelperNltk as th

logger = logging.getLogger('docHelper')

def getDocEnglishTitle(doc):
  docLang = doc.tags[FEEDTAG_LANG]

  if docLang != LANG_ENGLISH:
    return doc.tags[DOCTAG_TRANSLATED_TITLE]
  else:
    return doc.tags[LINKTAG_TITLE]

def getDocEnglishSummaryText(doc):
  docLang = doc.tags[FEEDTAG_LANG]

  if docLang != LANG_ENGLISH:
    return doc.tags[DOCTAG_TRANSLATED_SUMMARYTEXT]
  else:
    return doc.tags[LINKTAG_SUMMARYTEXT]

def getDocEnglishContent(doc):
  docLang = doc.tags[FEEDTAG_LANG]

  if docLang != LANG_ENGLISH:
    return doc.tags[DOCTAG_TRANSLATED_CONTENT]
  else:
    return doc.content

def getDocEnglishContentSimilarity(doc1, doc2):
    return th.compareUsingShingles(
        getDocEnglishContent(doc1),
        getDocEnglishContent(doc2))

def computeEnglishDocsSimScore(jobInfo, doc1, doc2):
    doc1EntityWeights = json.loads(doc1.tags.get(DOCTAG_ENTITY_WEIGHTS, "{}"))
    doc2EntityWeights = json.loads(doc2.tags.get(DOCTAG_ENTITY_WEIGHTS, "{}"))

    titleSim = th.compareTitles(
        getDocEnglishTitle(doc1),
        getDocEnglishTitle(doc2))

    titleSimEntities = th.compareTextEntities(
        getDocEnglishTitle(doc1),
        getDocEnglishTitle(doc2),
        doc1EntityWeights,
        doc2EntityWeights)

    titleAndSummarySimEntities = th.compareTextEntities(
        getDocEnglishTitle(doc1) + ". " + getDocEnglishSummaryText(doc1),
        getDocEnglishTitle(doc2) + ". " + getDocEnglishSummaryText(doc2),
        doc1EntityWeights,
        doc2EntityWeights)

    contentSim = getDocEnglishContentSimilarity(doc1, doc2)

    contentSimEntities = th.compareTextEntities(
        getDocEnglishContent(doc1),
        getDocEnglishContent(doc2),
        doc1EntityWeights,
        doc2EntityWeights)

    logger.info(
        "Doc comparision scores. titleSim: %s, titleSimEntities: %s," +
        "titleAndSummarySimEntities: %s, contentSim: %s, contentSimEntities: %s. %s",
        titleSim,
        titleSimEntities,
        titleAndSummarySimEntities,
        contentSim,
        contentSimEntities,
        jobInfo)

    return titleSim*0.1 \
           + titleSimEntities*0.15 \
           + titleAndSummarySimEntities * 0.3 \
           + contentSim*0.3 \
           + contentSimEntities*0.15

def computeDocSimScoreUsingEntities(jobInfo, doc1, doc2):
    doc1EntityWeights = json.loads(doc1.tags.get(DOCTAG_ENTITY_WEIGHTS, "{}"))
    doc2EntityWeights = json.loads(doc2.tags.get(DOCTAG_ENTITY_WEIGHTS, "{}"))

    titleSim = th.compareTitles(
        getDocEnglishTitle(doc1),
        getDocEnglishTitle(doc2))

    titleSimEntities = th.compareTextEntities(
        getDocEnglishTitle(doc1),
        getDocEnglishTitle(doc2),
        doc1EntityWeights,
        doc2EntityWeights)

    titleAndSummarySimEntities = th.compareTextEntities(
        getDocEnglishTitle(doc1) + ". " + getDocEnglishSummaryText(doc1),
        getDocEnglishTitle(doc2) + ". " + getDocEnglishSummaryText(doc2),
        doc1EntityWeights,
        doc2EntityWeights)

    contentSim = th.compareTextEntities(
        getDocEnglishContent(doc1),
        getDocEnglishContent(doc2),
        doc1EntityWeights,
        doc2EntityWeights)

    logger.info(
        "Doc comparision scores. titleSim: %s, titleSimEntities: %s," +
        "titleAndSummarySimEntities: %s, contentSim: %s. %s",
        titleSim,
        titleSimEntities,
        titleAndSummarySimEntities,
        contentSim,
        jobInfo)

    score = titleSim*0.2 \
           + titleSimEntities*0.3 \
           + titleAndSummarySimEntities*0.2 \
           + contentSim*0.3

    if score > 0.5:
        score = min(score * 1.4, 1.0)

    return score

def getDocComparisionScore(jobInfo, doc1, doc2):
    score = 0
    if (doc1.tags[FEEDTAG_LANG] == LANG_ENGLISH) and \
        (doc2.tags[FEEDTAG_LANG] == LANG_ENGLISH):
        score = computeEnglishDocsSimScore(jobInfo, doc1, doc2)
        logger.info("Compared using shingles. %s", jobInfo)
    else:
        score = computeDocSimScoreUsingEntities(jobInfo, doc1, doc2)
        # make it slightly easier for non-english docs to get clustered.
        score = score * 1.15
        logger.info("Compared using entities. %s", jobInfo)

    if FEEDTAG_LOCALE in doc1.tags and FEEDTAG_LOCALE in doc2.tags and \
        doc1.tags[FEEDTAG_LOCALE] != doc2.tags[FEEDTAG_LOCALE]:

        logger.info(
            "The two docs are from different locations. Adding penalty. %s",
            jobInfo)
        score = score - 0.4
        if score < 0:
            score = 0

    if doc1.tags[TAG_PUBLISHER] != doc2.tags[TAG_PUBLISHER]:
        # make it slightly easier for different-publisher-docs to get clustered
        score = score * 1.1

    doc1Publisher = doc1.tags[TAG_PUBLISHER]
    doc2Publisher = doc2.tags[TAG_PUBLISHER]
    if doc1Publisher == doc2Publisher:
        penaltyPublishers = ['TOI', 'TelanganaToday', 'Hindu']
        if doc1Publisher in penaltyPublishers :
            logger.info(
                "Adding penalty for same publisher %s. %s",
                doc1Publisher,
                jobInfo)
            score = score*0.75

    if score > 1:
        score = 1

    logger.info("Comparision score: %s. %s", str(score), jobInfo)
    return score