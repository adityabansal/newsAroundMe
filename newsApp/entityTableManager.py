import os
import time

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex

from .dbhelper import *
from .encodedEntity import EncodedEntity

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
        self.__table = None

    def __getTable(self):
        """
        Get the entity table.
        """

        if not self.__table:
            tableConnectionParams = parseConnectionString(
                self.tableConnString);

            self.__table = Table(
                tableConnectionParams['name'],
                connection = getDbConnection(tableConnectionParams));

        return self.__table;

    def __removeDuplicateEntities(self, entities):
        """
        Remove entities which might result in duplicate encodings.
        """

        seenEncodedEntities = set()
        uniqueEntitiesList = []

        for entity in entities:
            encodedEntity = EncodedEntity(entity).encoded
            if encodedEntity not in seenEncodedEntities:
                seenEncodedEntities.add(encodedEntity)
                uniqueEntitiesList.append(entity)

        return uniqueEntitiesList

    def createFreshTable(self):
        """
        Create a fresh empty entity table.
        """

        # delete existing table if it exists
        try:
            self.__getTable().delete();
            time.sleep(10)
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
                'write': 4,
            }, global_indexes = [
                GlobalAllIndex('docIdIndex', parts = [
                    HashKey('docId'),
                    RangeKey('entity')
                ],
                throughput = {
                    'read': 1,
                    'write': 4,
                })
            ],
            connection = getDbConnection(tableConnectionParams))

    def addEntries(self, docId, entities):
        """
        Add entries in entities table for entities and docId passed.
        """

        entities = self.__removeDuplicateEntities(entities)
        table = self.__getTable()
        with table.batch_write() as batch:
            for entity in entities:
                encodedEntity = EncodedEntity(entity)
                if encodedEntity.encoded:
                    batch.put_item(data={
                        'docId': docId,
                        'entity': encodedEntity.encoded,
                        'plain': encodedEntity.plain},
                        overwrite = True)

    def getEntityWeight(self, entity):
        encodedEntity = EncodedEntity(entity)
        if not encodedEntity.encoded:
            return 0.0

        docCount = len(list(
            self.queryByEntity(encodedEntity.encoded)))
        if docCount > 50:
           return 0.0;
        else:
           return (50.0 - docCount)/50

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
