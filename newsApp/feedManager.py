import os
import time

from .constants import *
from .feed import Feed
from .dbItemManagerV2 import DbItemManagerV2

DEFAULT_FEED_POLLING_FREQUENCY = 10

class FeedManager(DbItemManagerV2):
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

        DbItemManagerV2.__init__(self,
            os.environ['FEEDTAGSTABLE_CONNECTIONSTRING'])

    def put(self, feed):
        """
        Put a new feed.
        """

        # add polling info tags and put into database
        feed.tags[FEEDTAG_NEXTPOLLTIME] = int(time.time())
        if FEEDTAG_POLLFREQUENCY not in feed.tags:
            feed.tags[FEEDTAG_POLLFREQUENCY] = DEFAULT_FEED_POLLING_FREQUENCY
        DbItemManagerV2.put(self, feed)

    def getStaleFeeds(self):
        """
        Returns a list of feedIds of stale feeds (i.e whose next poll time
        is less than current time.
        """

        currentTime = int(time.time())
        scanResults = DbItemManagerV2.scan(self, nextPollTime__lte = currentTime)
        return (result.id for result in scanResults)

    def updateFeedOnSuccessfullPoll(self, feed):
        """
        Updates the polling related tags of specified feed and puts in db.
        """

        feed.tags[FEEDTAG_LASTPOLLTIME] = int(time.time())
        feed.tags[FEEDTAG_NEXTPOLLTIME] = (feed.tags[FEEDTAG_LASTPOLLTIME] +
            feed.tags[FEEDTAG_POLLFREQUENCY]*60)
        DbItemManagerV2.put(self, feed)
