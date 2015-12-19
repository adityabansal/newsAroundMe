import os

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey

from cluster import Cluster
from dbhelper import *

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

  def __getTable(self):
    """
    Get the clusters table.
    """

    tableConnectionParams = parseConnectionString(
      self.tableConnString);

    return Table(
      tableConnectionParams['name'],
      connection = getDbConnection(tableConnectionParams));

  def createFreshTable(self):
    """
    Create a fresh empty clusters table.
    """

    # create new table
    tableConnectionParams = parseConnectionString(self.tableConnString);
    return Table.create(
      tableConnectionParams['name'],
      schema = [HashKey('clusterId')],
      throughput = {
          'read': 1,
          'write': 1,
      }, connection = getDbConnection(tableConnectionParams))

  def deleteTable(self):
    """
    Delete table
    """

    self.__getTable().delete();

  def addClusters(self, clusters):
    table = self.__getTable();

    with table.batch_write() as batch:
      for cluster in clusters:
        batch.put_item(data={
          'clusterId': cluster.id,
          'docKeys': str(list(cluster)),
          'categories': str(cluster.categories),
          'countries': str(cluster.countries),
          'locales': str(cluster.locales),
          'publishers': str(cluster.publishers),
          'languages': str(cluster.languages)})

  def getCluster(self, clusterId):
    table = self.__getTable();
    queryResult = list(table.query_2(clusterId__eq = clusterId))

    if queryResult:
      return self.__getClusterFromTableRow(queryResult[0])
    else:
      return None

  def queryByCategoryAndCountry(self, category, country):
    table = self.__getTable()

    return (self.__getClusterFromTableRow(row) for row in table.scan(
      categories__contains = category,
      countries__contains = country))

  def queryByLocale(self, locale):
    table = self.__getTable()

    return (self.__getClusterFromTableRow(row) for row in table.scan(
      locales__contains = locale))

  def __getClusterFromTableRow(self, row):
    cluster = Cluster(eval(row['docKeys']))
    cluster.categories = eval(row['categories'])
    cluster.countries = eval(row['countries'])
    cluster.locales = eval(row['locales'])
    cluster.publishers = eval(row['publishers'])
    cluster.languages = eval(row['languages'])

    return cluster
