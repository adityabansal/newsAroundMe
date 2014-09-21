#TODO: make separate class for exceptions and raise them instead of generic

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey

from newsApp.dbItem import DbItem
from newsApp.dbhelper import *

class DbItemManager:
    """
    Manage DbItems stored on AWS dynamo db database.

    Contains functions for CRUD operations on the DbItems stored
    """

    def __init__(self, tableConnString):
        """
        Instantiates a new instance of DbItemManager class

        'TableConnString' : connection string of the table.
        """

        self.tableConnString = tableConnString;

    def __getTables(self):
        """
        Get the tables.
        """

        tagsTableConnectionParams = parseConnectionString(
            self.tableConnString);

        return Table(
            tagsTableConnectionParams['name'],
            schema = [HashKey('itemId'), RangeKey('tagName')],
            connection = getDbConnection(tagsTableConnectionParams))

    def __getTagsFromTagsTableRows(self, tagsTableRows):
        """
        Convert data retrieved from tagsTable to a dictionary
        """

        tags = {}

        for tag in tagsTableRows:
            tags[tag['tagName']] = tag['tagValue']

        return tags

    def __getItemTags(self, tagsTable, itemId):
        """
        Get tags as a dictionary given tagsTable and db item Id
        """

        tagsTableRows = tagsTable.query_2(itemId__eq = itemId)
        return self.__getTagsFromTagsTableRows(tagsTableRows)

    def put(self, item):
        """
        Put a new item.
        """

        tagsTable = self.__getTables()

        with tagsTable.batch_write() as batch:
            for tagName in item.tags:
                batch.put_item(data = {
                    'itemId' : item.id,
                    'tagName' : tagName,
                    'tagValue' : item.tags[tagName]})

    def get(self, itemId):
        """
        Get item with the specified Id.
        """

        tagsTable = self.__getTables()

        itemTags = self.__getItemTags(tagsTable, itemId)
        if not itemTags:
            raise Exception("dbItem not found")

        return DbItem(itemId, itemTags)

    def getEntriesWithTag(self, tagName):
        """
        Get all entries with specified tagName.
        """

        tagsTable = self.__getTables()
        return tagsTable.scan(tagName__eq = tagName)

    def delete(self, itemId):
        """
        Delete item with the specified Id from the databases.
        """

        tagsTable = self.__getTables()

        itemTags = self.__getItemTags(tagsTable, itemId)
        for itemTagName in itemTags:
            tagsTable.delete_item(
                itemId=itemId,
                tagName=itemTagName)

        return
