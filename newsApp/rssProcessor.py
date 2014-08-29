import time

import feedparser
from newsApp.feed import Feed
from newsApp.feedManager import FeedManager
from newsApp.linkManager import LinkManager

def _retrieveNewTagsFromFeedEntry(entry)
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

def _linkFromFeedEntry(entry, feed)
  """
  Creates a link from a feed entry and feed objects.
  """

  # Propogate tags from feed to link object
  linkTags = feed.tags

  # Add new tags retrieved from the feed entry
  linkTags.update(_retrieveNewTagsFromFeedEntry(entry))

  # Return the final link object
  return Link(entry.link, linkTags)

def processFeed(feedId, lastProcessedTime):
  """
  Processes a feed (takes as input the feedId and the time feed was last
  processed successfully)

  Steps:
  1. get Feed from database
  2. get all feed entries published since lastProcessedTime
  3. put the entries into the links database

  Returns a new timestamp upto which entries in feed were correctly processed
  """

  # get the feed
  feedManager = FeedManager()
  feed = feedManager.getFeed(feedId)

  # get all feed entries since last retrieved time
  parsedFeed = feedparser.parse(feed.tags['url'])
  newEntries = [entry for entry in parsedFeed.entries
               if time.mktime(entry.published_parsed) > lastProcessedTime)

  # put the entries into links database
  linkManager = LinkManager()
  for entry in newEntries
    link = _linkFromFeedEntry(entry, feed)
    linkManager.put(link)
