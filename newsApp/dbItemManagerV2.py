#TODO: make separate class for exceptions and raise them instead of generic
import decimal

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey
from retrying import retry

from dbItem import DbItem
from dbhelper import *

class DbItemManagerV2:
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

    def __getTable(self):
        """
        Get the table.
        """

        tableConnectionParams = parseConnectionString(
            self.tableConnString);

        return Table(
            tableConnectionParams['name'],
            schema = [HashKey('itemId')],
            connection = getDbConnection(tableConnectionParams))

    def __getItemFromTableRow(self, tableRow):
        tags  = {}

        for field, value in tableRow.items():
            if field != 'itemId':
                # boto retrieves numbers as decimals. Convert them to float
                # else we'll have json serialization issues down the pipeline
                if type(value) is decimal.Decimal:
                    tags[field] = float(value)
                else:
                    tags[field] = value

        return DbItem(tableRow['itemId'], tags)

    def __queryTableRow(self, itemId):

        table = self.__getTable()
        return table.get_item(itemId = itemId)

    @retry(stop_max_attempt_number=3)
    def put(self, item):
        """
        Put a new item.
        """

        tableData = {}
        tableData['itemId'] = item.id
        for tagName in item.tags:
            tableData[tagName] = item.tags[tagName];

        table = self.__getTable();
        table.put_item(data = tableData, overwrite = True)

    def get(self, itemId):
        """
        Get item with the specified Id.
        """

        tableRow = self.__queryTableRow(itemId)
        return self.__getItemFromTableRow(tableRow)

    def scan(self, **filters):
        table = self.__getTable()
        scanResults = table.scan(**filters);
        return (self.__getItemFromTableRow(row) for row in scanResults)

    def delete(self, itemId):
        """
        Delete item with the specified Id from the database.
        """

        table = self.__getTable()
        table.delete_item(itemId = itemId)