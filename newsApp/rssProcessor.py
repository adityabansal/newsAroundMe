import calendar
import json
import time
import logging

import feedparser

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
  FEEDTAG_LASTPUBDATE]

logger = logging.getLogger('rssProcessor')

def _deleteUnecessaryFeedTags(feedTags):
    """
    Deletes tags which need not be proppogated from a feed to a link.
    """

    for tagName in UNECESSARY_FEED_TAGS:
        feedTags.pop(tagName, None)

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

def processFeed(jobId, feedId):
  """
  Processes a feed (takes as input the feedId)

  Steps:
  1. get Feed from database
  2. get all feed entries published since lastPubDate
  3. put the entries into the links database
  """

  feedAndJobId = "Feed id: " + feedId + ". Job id: " + jobId;
  logger.info("Started processing rss feed. %s.", feedAndJobId)
  
  # get the feed
  feedManager = FeedManager()
  feed = feedManager.get(feedId)
  logger.info("Got feed from database. %s.", feedAndJobId)

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
  linkManager = LinkManager()
  jobManager = MinerJobManager()
  for entry in newEntries:
    link = _linkFromFeedEntry(jobId, entry, feed)
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

  # last step update the feed on successful completion of poll
  if len(newEntries) > 0:
    feed.tags[FEEDTAG_LASTPUBDATE] = calendar.timegm(
      newEntries[0].published_parsed)
  feedManager.updateFeedOnSuccessfullPoll(feed)
  logger.info(
    "Feed updated after being successfully processed. %s.",
    feedAndJobId)

  logger.info("Completed processing rss feed. %s.", feedAndJobId)
