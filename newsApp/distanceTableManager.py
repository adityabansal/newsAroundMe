import os
import time

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex
from dbhelper import *

class DistanceTableManager:
    """
    Manage doc distance pairs stored on AWS dynamo db database.

    Contains functions for CRUD on table, adding and querying entries etc.
    """

    def __init__(self):
        """
        Instantiates a new instance of DbItemManager class
        """

        self.tableConnString = os.environ['DISTTABLE_CONNECTIONSTRING'];

    def __getTable(self):
        """
        Get the distances table.
        """

        tableConnectionParams = parseConnectionString(
            self.tableConnString);

        return Table(
            tableConnectionParams['name'],
            connection = getDbConnection(tableConnectionParams));

    def createFreshTable(self):
        """
        Create a fresh empty distance table.
        """

        # delete existing table if it exists
        try:
            self.__getTable().delete();
            time.sleep(5)
        except:
            pass;# do nothing. Maybe there was no existing table

        # create new table
        tableConnectionParams = parseConnectionString(
            self.tableConnString);
        return Table.create(
            tableConnectionParams['name'],
            schema = [
                HashKey('from'),
                RangeKey('to')
            ], throughput = {
                'read': 1,
                'write': 2,
            }, global_indexes = [
                GlobalAllIndex('reverseIndex', parts = [
                    HashKey('to'),
                    RangeKey('from')
                ],
                throughput = {
                    'read': 1,
                    'write': 2,
                })
            ],
            connection = getDbConnection(tableConnectionParams))

    def addEntry(self, docId1, docId2, distance):
        """
        Add a entry in the distances table.
        """

        table = self.__getTable();

        table.put_item(data = {
            'from' : min(docId1, docId2),
            'to' : max(docId1, docId2),
            'distance' : str(distance)},
            overwrite = True)

    def getEntries(self):
        """
        Get the distance entries.
        Return results in a generator
        """

        table = self.__getTable();
        scanResults = table.scan();
        return ((result['from'], result['to'], eval(result['distance']))
                for result in scanResults)

    def getDistanceMatrix(self):
        """
        Get the distance entries in matrix form.
        """

        entries = self.getEntries()
        dMatrix = {}

        for entry in entries:
            if entry[0] not in dMatrix:
                dMatrix[entry[0]] = {}
            dMatrix[entry[0]][entry[1]] = entry[2]

        return dMatrix

    def cleanUpDoc(self, docId):
        """
        Cleanup a document from the distance table
        """

        docEntries = self.__queryByDocId(docId)

        table = self.__getTable();
        with table.batch_write() as batch:
            for entry in docEntries:
                batch.delete_item(**{
                    'from': entry['from'],
                    'to': entry['to']
                })

    def deleteEntry(self, docId1, docId2):
        """
        Delete a entry from the distances table.
        """

        table = self.__getTable();

        table.delete_item(**{
            'from': min(docId1, docId2),
            'to': max(docId1, docId2)
        })

    def __queryByDocId(self, docId):
        table = self.__getTable();

        entries = list(table.query_2(from__eq = docId)) +\
            list(table.query_2(to__eq = docId, index = 'reverseIndex'))

        return entries
