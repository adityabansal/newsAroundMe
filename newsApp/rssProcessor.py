import time
import logging

import feedparser

from constants import *
from feed import Feed
from feedManager import FeedManager
from link import Link
from linkManager import LinkManager

UNECESSARY_FEED_TAGS = [FEEDTAG_TYPE, FEEDTAG_NEXTPOLLTIME, FEEDTAG_POLLFREQUENCY, FEEDTAG_LASTPOLLTIME, FEEDTAG_URL]

logger = logging.getLogger('rssProcessor')

def _deleteUnecessaryFeedTags(feedTags):
    """
    Deletes tags which need not be proppogated from a feed to a link.
    """

    for tagName in UNECESSARY_FEED_TAGS:
        feedTags.pop(tagName, None)

def _retrieveNewTagsFromFeedEntry(entry):
  """
  Process the summary detail of rss feed entry.
  Computes tags for the link object being prepared from the feed entry.
  """

  newTags = {};

  # add title
  newTags['title'] = entry.title

  # plan is to do some html processing here to extract relevant text from
  # summary before putting it.
  newTags['summary'] = entry.summary

  return newTags

def _linkFromFeedEntry(entry, feed):
  """
  Creates a link from a feed entry and feed objects.
  """

  # Propogate tags from feed to link object
  linkTags = dict(feed.tags)
  _deleteUnecessaryFeedTags(linkTags)

  # Add new tags retrieved from the feed entry
  linkTags.update(_retrieveNewTagsFromFeedEntry(entry))

  # Return the final link object
  return Link(entry.link, linkTags)

def processFeed(feedId):
  """
  Processes a feed (takes as input the feedId and the time feed was last
  processed successfully)

  Steps:
  1. get Feed from database
  2. get all feed entries published since lastPollTime
  3. put the entries into the links database

  Returns a new timestamp upto which entries in feed were correctly processed
  """

  logger.info("Started processing rss feed. Feed id: %s.", feedId)
  
  # get the feed
  feedManager = FeedManager()
  feed = feedManager.get(feedId)

  # compute the last poll time
  lastPollTime = 0;
  if FEEDTAG_LASTPOLLTIME in feed.tags:
      lastPollTime = feed.tags[FEEDTAG_LASTPOLLTIME]

  # get all feed entries since last poll time
  parsedFeed = feedparser.parse(feed.tags[FEEDTAG_URL])
  newEntries = [entry for entry in parsedFeed.entries
               if entry.published_parsed > time.gmtime(lastPollTime)]
  logger.info("Got %i new entries.", len(newEntries))

  # put the entries into links database
  linkManager = LinkManager()
  for entry in newEntries:
    link = _linkFromFeedEntry(entry, feed)
    linkManager.put(link)
    logger.info("Put link with id '%s' in links database", link.id)

  # last step update the feed on successful completion of poll
  feedManager.updateFeedOnSuccessfullPoll(feed)

  logger.info("Completed processing rss feed. Feed id: %s.", feedId)
