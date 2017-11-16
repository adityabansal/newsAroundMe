import os
import time

from constants import *
from dbhelper import *
from dbItemManagerV2 import DbItemManagerV2
from link import Link

LINK_EXPIRY_TIME_IN_DAYS = 80

class LinkManager(DbItemManagerV2):
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

        DbItemManagerV2.__init__(self,
            os.environ['LINKTAGSTABLE_CONNECTIONSTRING'])

    def get(self, linkId):
        """
        Put a new link.
        """

        dbItem = DbItemManagerV2.get(self, linkId);
        return Link(linkId, dbItem.tags)

    def getStaleLinks(self):
        """
        Returns a list of linkIds of stale links.
        """

        linkExpiryCutoff = int(time.time()) - LINK_EXPIRY_TIME_IN_DAYS*24*60*60;
        scanResults = DbItemManagerV2.scan(self, pubtime__lte = linkExpiryCutoff)
        return (result.id for result in scanResults)

    def getUnprocessedLinks(self):
        return DbItemManagerV2.query_2(
            self,
            isProcessed__eq = 'false',
            index = 'isProcessed-itemId-index')
