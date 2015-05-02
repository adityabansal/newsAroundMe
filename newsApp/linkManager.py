import os
import time

from constants import *
from dbhelper import *
from dbItemManager import DbItemManager

LINK_EXPIRY_TIME_IN_DAYS = 30

class LinkManager(DbItemManager):
    """
    Manage links stored on AWS dynamo db database.

    Contains functions for CRUD operations on the links stored

    Following environment variables need to be set -
    'LINKTAGSTABLE_CONNECTIONSTRING' : connection string of link tags table.
    """

    def __init__(self):
        """
        Instantiates the linkManager.
        """

        DbItemManager.__init__(self,
            os.environ['LINKTAGSTABLE_CONNECTIONSTRING'])

    def getStaleLinks(self):
        """
        Returns a list of linkIds of stale links.
        """

        scanResults = DbItemManager.getEntriesWithTag(self, LINKTAG_PUBTIME)
        return [result['itemId'] for result in scanResults
            if result['tagValue'] + LINK_EXPIRY_TIME_IN_DAYS*24*60*60 \
                < int(time.time())]
