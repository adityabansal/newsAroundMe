import os

from newsApp.feed import Feed
from newsApp.dbhelper import *
from newsApp.dbItemManager import DbItemManager

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
