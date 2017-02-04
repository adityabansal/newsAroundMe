import os
import time

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex

from dbhelper import *

MAX_SHINGLES_PER_DOC = 300;

class ShingleTableManager:
    """
    Manage shingle-docid pairs stored on AWS dynamo db database.

    Contains functions for CRUD on table, adding and querying entries etc.
    """

    def __init__(self):
        """
        Instantiates a new instance of DbItemManager class
        """

        self.tableConnString = os.environ['SHINGLETABLE_CONNECTIONSTRING'];

    def __getTable(self):
        """
        Get the shingle table.
        """

        shingleTableConnectionParams = parseConnectionString(
            self.tableConnString);

        return Table(
            shingleTableConnectionParams['name'],
            connection = getDbConnection(shingleTableConnectionParams));

    def createFreshTable(self):
        """
        Create a fresh empty shingle table.
        """

        # delete existing table if it exists
        try:
            self.__getTable().delete();
            time.sleep(10)
        except:
            pass;# do nothing. Maybe there was no existing table

        # create new table
        shingleTableConnectionParams = parseConnectionString(
            self.tableConnString);
        return Table.create(
            shingleTableConnectionParams['name'],
            schema = [
                HashKey('shingle'),
                RangeKey('docId')
            ], throughput = {
                'read': 14,
                'write': 20,
            }, global_indexes = [
                GlobalAllIndex('docIdIndex', parts = [
                    HashKey('docId'),
                    RangeKey('shingle')
                ],
                throughput = {
                    'read': 1,
                    'write': 20,
                })
            ],
            connection = getDbConnection(shingleTableConnectionParams))

    def addEntries(self, docId, shingles):
        """
        Add a entries in shingles table for shingles and docId passed.
        """

        shingles = list(set(shingles)) # remove duplicate shingles
        shingles.sort()
        shingles = shingles[:MAX_SHINGLES_PER_DOC]

        shingleTable = self.__getTable()
        with shingleTable.batch_write() as batch:
            for shingle in shingles:
                batch.put_item(data={
                    'docId': docId,
                    'shingle': shingle})

    def queryByShingle(self, shingle):
        """
        Retrieve list of docId's for the passed shingle.
        """

        shingleTable = self.__getTable()
        return (row['docId'] for row in shingleTable.query_2(
            shingle__eq = shingle))

    def queryByDocId(self, docId):
        """
        Retrieve list of shingles for the passed docId.
        """

        shingleTable = self.__getTable()
        return (row['shingle'] for row in shingleTable.query_2(
            docId__eq = docId,
            index = 'docIdIndex'))

    def cleanUpDocShingles(self, docId):
        """
        Cleanup all shingles of doc.
        """

        shingles = self.queryByDocId(docId)
        shingleTable = self.__getTable()

        with shingleTable.batch_write() as batch:
            for shingle in shingles:
                batch.delete_item(docId=docId, shingle=shingle)
