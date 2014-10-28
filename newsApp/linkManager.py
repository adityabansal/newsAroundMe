import os

from dbhelper import *
from dbItemManager import DbItemManager

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
