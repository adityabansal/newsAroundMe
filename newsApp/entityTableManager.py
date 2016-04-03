import os
import time

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex

from dbhelper import *
from encodedEntity import EncodedEntity

class EntityTableManager:
    """
    Manage entity-docid pairs stored on AWS dynamo db database.

    Contains functions for CRUD on table, adding and querying entries etc.
    """

    def __init__(self):
        """
        Instantiates a new instance of EntityTableManager class
        """

        self.tableConnString = os.environ['ENTITYTABLE_CONNECTIONSTRING'];

    def __getTable(self):
        """
        Get the entity table.
        """

        tableConnectionParams = parseConnectionString(
            self.tableConnString);

        return Table(
            tableConnectionParams['name'],
            connection = getDbConnection(tableConnectionParams));

    def createFreshTable(self):
        """
        Create a fresh empty entity table.
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
                HashKey('entity'),
                RangeKey('docId')
            ], throughput = {
                'read': 1,
                'write': 1,
            }, global_indexes = [
                GlobalAllIndex('docIdIndex', parts = [
                    HashKey('docId'),
                    RangeKey('entity')
                ],
                throughput = {
                    'read': 1,
                    'write': 1,
                })
            ],
            connection = getDbConnection(tableConnectionParams))

    def addEntries(self, docId, entities):
        """
        Add entries in entities table for entities and docId passed.
        """

        entities = list(set(entities)) # remove duplicate
        table = self.__getTable()
        with table.batch_write() as batch:
            for entity in entities:
                encodedEntity = EncodedEntity(entity)
                batch.put_item(data={
                    'docId': docId,
                    'entity': encodedEntity.encoded,
                    'plain': encodedEntity.plain},
                    overwrite = True)

    def queryByEntity(self, entity):
        """
        Retrieve list of docId's for the passed entity.
        """

        table = self.__getTable()
        return (row['docId'] for row in table.query_2(
            entity__eq = entity))

    def queryByDocId(self, docId):
        """
        Retrieve list of entities for the passed docId.
        """

        table = self.__getTable()
        return (row['entity'] for row in table.query_2(
            docId__eq = docId,
            index = 'docIdIndex'))

    def cleanUpDocEntities(self, docId):
        """
        Cleanup all entities of doc.
        """

        entities = self.queryByDocId(docId)
        table = self.__getTable()

        with table.batch_write() as batch:
            for entity in entities:
                batch.delete_item(docId=docId, entity=entity)
