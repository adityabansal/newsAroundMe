import os

from newsApp.feed import Feed
from newsApp.dbhelper import *
from newsApp.dbItemManager import DbItemManager

class FeedManager:
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

        self.dbItemManager = DbItemManager(
            os.environ['FEEDTAGSTABLE_CONNECTIONSTRING'])

    def putFeed(self, feed):
        """
        Put a new feed into the databases
        """

        self.dbItemManager.putItem(feed)

    def getFeed(self, feedId):
        """
        Get feed with the specified feedId from the databases.
        """

        return self.dbItemManager.getItem(feedId)

    def deleteFeed(self, feedId):
        """
        Delete feed with the specified feedId from the databases.
        """

        self.dbItemManager.deleteItem(feedId);
