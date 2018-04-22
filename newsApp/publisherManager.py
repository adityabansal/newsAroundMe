import os

from publisher import Publisher
from dbItemManagerV2 import DbItemManagerV2

class PublisherManager(DbItemManagerV2):
    """
    Manage publishers stored on AWS dynamo db database.

    Contains functions for CRUD operations on the publishers stored

    Following environment variables need to be set -
    'PUBLISHERTAGSTABLE_CONNECTIONSTRING' : connection string of publisher
    tags table.
    """

    def __init__(self):
        """
        Instantiates the publisherManager.
        """

        DbItemManagerV2.__init__(self,
            os.environ['PUBLISHERTAGSTABLE_CONNECTIONSTRING']);
