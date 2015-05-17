import os
import time

from constants import *
from feed import Feed
from dbItemManager import DbItemManager

DEFAULT_FEED_POLLING_FREQUENCY = 10

class FeedManager(DbItemManager):
    """
    Manage feeds stored on AWS dynamo db database.

    Contains functions for CRUD operations on the feeds stored

    Following environment variables need to be set -
    'FEEDTAGSTABLE_CONNECTIONSTRING' : connection string of feed tags table.
    """

    def __init__(self):
        """
        Instantiates the feedManager.
        """

        DbItemManager.__init__(self,
            os.environ['FEEDTAGSTABLE_CONNECTIONSTRING'])

    def put(self, feed):
        """
        Put a new feed.
        """

        # add polling info tags and put into database
        feed.tags[FEEDTAG_NEXTPOLLTIME] = int(time.time())
        feed.tags[FEEDTAG_POLLFREQUENCY] = DEFAULT_FEED_POLLING_FREQUENCY
        DbItemManager.put(self, feed)

    def getStaleFeeds(self):
        """
        Returns a list of feedIds of stale feeds (i.e whose next poll time
        is less than current time.
        """

        scanResults = DbItemManager.getEntriesWithTag(self,
            FEEDTAG_NEXTPOLLTIME)
        return (result['itemId'] for result in scanResults
            if result['tagValue'] < int(time.time()))

    def updateFeedOnSuccessfullPoll(self, feed):
        """
        Updates the polling related tags of specified feed and puts in db.
        """

        feed.tags[FEEDTAG_LASTPOLLTIME] = int(time.time())
        feed.tags[FEEDTAG_NEXTPOLLTIME] = (feed.tags[FEEDTAG_LASTPOLLTIME] +
            feed.tags[FEEDTAG_POLLFREQUENCY]*60)
        DbItemManager.put(self, feed)
