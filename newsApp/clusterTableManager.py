import os
import time
import json

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey

from cluster import Cluster
from dbhelper import *
from constants import *

class ClusterTableManager:
  """
  Manage cluster entries stored on AWS dynamo db database.

  Contains functions for CRUD on table, adding and querying entries etc.
  """

  def __init__(self):
    """
    Instantiates a new instance of ClusterTableManager class
    """

    self.tableConnString = os.environ['CLUSTERTABLE_CONNECTIONSTRING'];
    self.docClusterMappingTable = os.environ['DOCCLUSTERMAPPINGTABLE_CONNECTIONSTRING']

  def __getTable(self):
    """
    Get the clusters table.
    """

    return getDbTable(self.tableConnString);

  def __getMappingsTable(self):
    return getDbTable(self.docClusterMappingTable)

  def archiveOldClusters(self):
    currentClusters = self.getCurrentClusters()
    table = self.__getTable();
    timeLimit = int(time.time()) - CLUSTERING_DOC_AGE_LIMIT * 60 * 60 * 24;

    clustersToArchive = [cluster for cluster in currentClusters if \
      cluster.lastPubTime < timeLimit]
    for cluster in clustersToArchive:
      cluster.isCurrent = 'false';
    self.addClusters(clustersToArchive);

    return clustersToArchive;

  def addClusters(self, clusters):
    table = self.__getTable();
    with table.batch_write() as batch:
      for cluster in clusters:
        batch.put_item(data={
          'clusterId': cluster.id,
          'docKeys': json.dumps(list(cluster)),
          'categories': json.dumps(cluster.categories),
          'countries': json.dumps(cluster.countries),
          'locales': json.dumps(cluster.locales),
          'publishers': json.dumps(cluster.publishers),
          'languages': json.dumps(cluster.languages),
          'duplicates': json.dumps(cluster.duplicates),
          'isCurrent': cluster.isCurrent,
          'lastPubTime': cluster.lastPubTime,
          'notifiedOnTwitter': cluster.notifiedOnTwitter
        })

    mappingsTable = self.__getMappingsTable()
    with mappingsTable.batch_write() as batch:
      for cluster in clusters:
        for doc in cluster:
          batch.put_item(data={
            'clusterId': cluster.id,
            'docId': doc
          })

  def addCluster(self, cluster):
    self.addClusters([cluster])

  def getCluster(self, clusterId):
    table = self.__getTable();
    queryResult = list(table.query_2(clusterId__eq = clusterId))

    if queryResult:
      return self.__getClusterFromTableRow(queryResult[0])
    else:
      return None

  def getAllClusters(self):
    table = self.__getTable()
    scanResult = table.scan()

    return (self.__getClusterFromTableRow(row) for row in scanResult)

  def getCurrentClusters(self):
    table = self.__getTable()

    return (self.__getClusterFromTableRow(row) for row in table.query_2(
      isCurrent__eq = 'true',
      index = 'isCurrent-clusterId-index'))

  def queryByCategoryAndCountry(self, category, country):
    table = self.__getTable()

    return (cluster for cluster in self.getCurrentClusters() \
      if category in cluster.categories and country in cluster.countries)

  def queryByLocale(self, locale):
    table = self.__getTable()

    return (cluster for cluster in self.getCurrentClusters() \
      if locale in cluster.locales)

  def queryByDocId(self, docId):
    mappingsTable = self.__getMappingsTable()
    mappingQueryResult = list(mappingsTable.query_2(
      docId__eq = docId, index = 'docId-clusterId-index'))

    if mappingQueryResult:
      return self.getCluster(mappingQueryResult[0]['clusterId'])
    else:
      return None

  def deleteClusters(self, clusters):
    mappingsTable = self.__getMappingsTable()
    with mappingsTable.batch_write() as batch:
      for cluster in clusters:
        for doc in cluster:
          batch.delete_item(clusterId = cluster.id, docId = doc)

    table = self.__getTable()
    with table.batch_write() as batch:
      for cluster in clusters:
        batch.delete_item(clusterId = cluster.id)

  def deleteCluster(self, cluster):
    self.deleteClusters([cluster])

  def __getClusterFromTableRow(self, row):
    cluster = Cluster(json.loads(row['docKeys']))
    cluster.categories = json.loads(row['categories'])
    cluster.countries = json.loads(row['countries'])
    cluster.locales = json.loads(row['locales'])
    cluster.publishers = json.loads(row['publishers'])
    cluster.languages = json.loads(row['languages'])
    cluster.duplicates = json.loads(row['duplicates'])
    cluster.isCurrent = row.get('isCurrent', 'unknown')
    cluster.lastPubTime = float(row.get('lastPubTime', 0))
    cluster.notifiedOnTwitter = row.get('notifiedOnTwitter', False)

    return cluster
