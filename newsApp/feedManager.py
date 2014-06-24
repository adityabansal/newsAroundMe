#TODO: make separate class for exceptions and raise them instead of generic
#      Exception.
#TODO: move the database column names and environment variable names to constants.

import boto.dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey
import os
from newsApp.feed import Feed

class FeedManager:
    """
    Manage feeds stored on AWS dynamo db database.

    Contains functions for CRUD operations on the feeds stored

    Following environment variables need to be set -
    'FEEDTABLES_REGION' : aws region in which the databases are present.
    'FEEDTABLE_NAME' : name of feed table database.
    'FEEDTAGSTABLE_NAME' : name of feed tags table database.
    'FEEDTABLES_ACCESS_KEY_ID' : aws access key id for the databases
    'FEEDTABLES_SECRET_ACCESS_KEY' : aws secret access key the databases
    """
    def __getConnection(self):
        """
        Get a dynamo db connection object using credentials from environment variables
        """

        connection = boto.dynamodb2.connect_to_region(
            os.environ['FEEDTABLES_REGION'],
            aws_access_key_id = os.environ['FEEDTABLES_ACCESS_KEY_ID'],
            aws_secret_access_key= os.environ['FEEDTABLES_SECRET_ACCESS_KEY'])
    
        return connection

    def __getTables(self):
        """
        Get the tables in which feed data is stored.
        """

        feedTable = Table(
            os.environ['FEEDTABLE_NAME'],
            schema = [HashKey('id')],
            connection = self.__getConnection())

        feedTagsTable = Table(
            os.environ['FEEDTAGSTABLE_NAME'],
            schema = [HashKey('feedId'), RangeKey('tagName')],
            connection = self.__getConnection())

        return feedTable, feedTagsTable

    def __getFeedTagsFromfeedTableRows(self, feedTableRows):
        """
        Convert data retrieved from feedTagTable to a dictionary
        """

        feedTags = {}

        for feedTag in feedTableRows:
            feedTags[feedTag['tagName']] = feedTag['tagValue']

        return feedTags

    def __getFeedTags(self, feedTagsTable, feedId):
        """
        Get Feed tags as a dictionary given feedTagsTable and feedId
        """
        feedTagsTableRows = feedTagsTable.query(feedId__eq = feedId)
        return self.__getFeedTagsFromfeedTableRows(feedTagsTableRows)

    def putFeed(self, feed):
        """
        Put a new feed into the databases
        """

        feedTable, feedTagsTable = self.__getTables()

        feedTable.put_item(data = {
            'id' : feed.id })
        
        with feedTagsTable.batch_write() as batch:
            for feedTagName in feed.tags:
                batch.put_item(data = {
                    'feedId' : feed.id,
                    'tagName' : feedTagName,
                    'tagValue' : feed.tags[feedTagName]})

    def getFeed(self, feedId):
        """
        Get feed with the specified feedId from the databases.
        """
        
        feedTable, feedTagsTable = self.__getTables()

        feedTags = self.__getFeedTags(feedTagsTable, feedId)
        if not feedTags:
            raise Exception("Feed not found")

        return Feed(feedId, feedTags)

    def deleteFeed(self, feedId):
        """
        Delete feed with the specified feedId from the databases.
        """

        feedTable, feedTagsTable = self.__getTables()

        feedTable.delete_item(id=feedId)
        
        feedTags = self.__getFeedTags(feedTagsTable, feedId)
        for feedTagName in feedTags:
            feedTagsTable.delete_item(
                feedId=feedId,
                tagName=feedTagName)

        return
