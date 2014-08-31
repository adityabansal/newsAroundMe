import os
import time

from newsApp.constants import *
from newsApp.feed import Feed
from newsApp.dbhelper import *
from newsApp.dbItemManager import DbItemManager

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
