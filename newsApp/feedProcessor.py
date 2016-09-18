import calendar
import json
import time
import logging

import feedparser
import requests

from constants import *
from feed import Feed
from feedManager import FeedManager
import htmlProcessor as hp
from minerJobManager import MinerJobManager
from link import Link
from linkManager import LinkManager
from workerJob import WorkerJob

UNECESSARY_FEED_TAGS = [
  FEEDTAG_TYPE,
  FEEDTAG_NEXTPOLLTIME,
  FEEDTAG_POLLFREQUENCY,
  FEEDTAG_LASTPOLLTIME,
  FEEDTAG_URL,
  FEEDTAG_LASTPUBDATE,
  FEEDTAG_ENTRY_SELECTORS]

logger = logging.getLogger('feedProcessor')

def _deleteUnecessaryFeedTags(feedTags):
    """
    Deletes tags which need not be proppogated from a feed to a link.
    """

    for tagName in UNECESSARY_FEED_TAGS:
        feedTags.pop(tagName, None)

def _putNewLinks(feedAndJobId, linksToAdd):
  linkManager = LinkManager()
  jobManager = MinerJobManager()

  for link in linksToAdd:
    try:
      existingLink = linkManager.get(link.id)
      link.tags[LINKTAG_PUBTIME] = existingLink.tags[LINKTAG_PUBTIME]
      logger.info(
        "Link with id '%s' already exists. Not processing it. %s",
        link.id,
        feedAndJobId)
      continue
    except:
      pass

    linkManager.put(link)
    logger.info(
        "Put link with id '%s' in links database. %s.",
        link.id,
        feedAndJobId)

    processLinkJob = WorkerJob(
        JOB_PROCESSLINK,
        { JOBARG_PROCESSLINK_LINKID : link.id})
    jobManager.enqueueJob(processLinkJob)
    logging.info(
        "Process link job with jobId '%s' put for linkId: %s. %s.",
        processLinkJob.jobId,
        link.id,
        feedAndJobId)

def _retrieveNewTagsFromFeedEntry(jobId, entry):
  """
  Process the summary detail of rss feed entry.
  Computes tags for the link object being prepared from the feed entry.
  """

  newTags = {};

  # add title
  newTags[LINKTAG_TITLE] = entry.title

  # add summary and image tags
  processingResult = hp.processHtml(
      jobId,
      entry.summary,
      ":not(script)",
      ["img"]);
  newTags[LINKTAG_SUMMARY] = entry.summary;
  newTags[LINKTAG_SUMMARYTEXT] = processingResult[0];
  newTags[LINKTAG_SUMMARYIMAGES] = processingResult[1];

  newTags[LINKTAG_PUBTIME] = calendar.timegm(entry.published_parsed);

  return newTags


def _linkFromFeedEntry(jobId, entry, feed):
  """
  Creates a link from a feed entry and feed objects.
  """

  # Propogate tags from feed to link object
  linkTags = dict(feed.tags)
  _deleteUnecessaryFeedTags(linkTags)

  # Add new tags retrieved from the feed entry
  linkTags.update(_retrieveNewTagsFromFeedEntry(jobId, entry))

  # Return the final link object
  return Link(entry.link, linkTags)

def processRssFeed(jobId, feed):
  """
  Processes a rss feed (takes as input the feed)
  """

  feedAndJobId = "Feed id: " + feed.id + ". Job id: " + jobId;

  # compute the last pubDate
  lastPubDate = 0;
  if FEEDTAG_LASTPUBDATE in feed.tags:
      lastPubDate = feed.tags[FEEDTAG_LASTPUBDATE]

  # get all feed entries since last poll time
  parsedFeed = feedparser.parse(feed.tags[FEEDTAG_URL])
  newEntries = [entry for entry in parsedFeed.entries
               if entry.published_parsed > time.gmtime(lastPubDate)]
  logger.info("Got %i new entries. %s", len(newEntries), feedAndJobId)

  # for each entry add link in link database and a process link job
  linksToAdd = []
  for entry in newEntries:
    link = _linkFromFeedEntry(jobId, entry, feed)
    linksToAdd.append(link);
  _putNewLinks(feedAndJobId, linksToAdd)

  # last step update the feed on successful completion of poll
  if len(newEntries) > 0:
    feed.tags[FEEDTAG_LASTPUBDATE] = calendar.timegm(
      newEntries[0].published_parsed)

  feedManager = FeedManager()
  feedManager.updateFeedOnSuccessfullPoll(feed)
  logger.info(
    "Feed updated after being successfully processed. %s.",
    feedAndJobId)

  logger.info("Completed processing rss feed. %s.", feedAndJobId)

def _linkFromWebPageEntry(jobId, entry, feed, entrySelector):
  """
  Creates a link from a web page entry.
  """

  # Propogate tags from feed to link object
  linkTags = dict(feed.tags)
  _deleteUnecessaryFeedTags(linkTags)

  # Try and extract the title. If unsuccessful, just return None.
  extractTitleResult = hp.extractLink(
    jobId,
    entry,
    entrySelector['title'],
    feed.tags[FEEDTAG_URL])
  if not extractTitleResult:
    return None;
  link = extractTitleResult[0];
  linkTags[LINKTAG_TITLE] = extractTitleResult[1];

  # add summary and image tags
  processingResult = hp.processHtml(
      jobId,
      entry,
      entrySelector['summary'],
      entrySelector['image'],
      feed.tags[FEEDTAG_URL]);
  linkTags[LINKTAG_SUMMARY] = entry;
  linkTags[LINKTAG_SUMMARYTEXT] = processingResult[0];
  linkTags[LINKTAG_SUMMARYIMAGES] = processingResult[1];

  linkTags[LINKTAG_PUBTIME] = int(time.time())

  # Return the final link object
  return Link(link, linkTags)

def processWebFeed(jobId, feed):
  feedAndJobId = "Feed id: " + feed.id + ". Job id: " + jobId;

  # get page html
  pageHtml = requests.get(feed.tags[FEEDTAG_URL]).text
  logger.info("Got html for web page. %s.", feedAndJobId)

  # load entry selectors
  entrySelectors = json.loads(feed.tags[FEEDTAG_ENTRY_SELECTORS])
  logger.info(
    "Will use %i entry selectors. %s",
    len(entrySelectors),
    feedAndJobId)

  # Use entry selector to get entries
  linksToAdd = []
  for entrySelector in entrySelectors:
    entries = hp.getSubHtmlEntries(jobId, pageHtml, entrySelector['overall'])
    logger.info(
      "Got %i entries for entry selector %s. %s",
      len(entries),
      entrySelector['overall'],
      feedAndJobId)

    for entry in entries:
      link = _linkFromWebPageEntry(jobId, entry, feed, entrySelector)
      if link:
        linksToAdd.append(link);
        logger.info("Discovered link: %s. %s", link.id, feedAndJobId)

  if len(linksToAdd) == 0:
    logger.warning("No links found while processing webPage. %s", feedAndJobId)
  else:
    logger.info("Number of links found: %i. %s", len(linksToAdd), feedAndJobId)

  # put links and processLink jobs
  _putNewLinks(feedAndJobId, linksToAdd)

  # update Feed on successfull poll
  feedManager = FeedManager()
  feedManager.updateFeedOnSuccessfullPoll(feed)
  logger.info(
    "Feed updated after being successfully processed. %s.",
    feedAndJobId)

  logger.info("Completed processing webPage feed. %s.", feedAndJobId)

def processFeed(jobId, feedId):
  feedAndJobId = "Feed id: " + feedId + ". Job id: " + jobId;
  logger.info("Started processing rss feed. %s.", feedAndJobId)

  # get the feed
  feedManager = FeedManager()
  feed = feedManager.get(feedId)
  logger.info("Got feed from database. %s.", feedAndJobId)

  if feed.tags[FEEDTAG_TYPE] == FEEDTYPE_RSS:
    processRssFeed(jobId, feed)
  elif feed.tags[FEEDTAG_TYPE] == FEEDTYPE_WEBPAGE:
    processWebFeed(jobId, feed)
