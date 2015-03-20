from publisher import Publisher
from dbItemManager import DbItemManager

class PublisherManager(DbItemManager):
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

        DbItemManager.__init__(self,
            os.environ['PUBLISHERTAGSTABLE_CONNECTIONSTRING']);
