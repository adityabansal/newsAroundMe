import json
import logging
import os

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key

from cluster import Cluster
from dbhelper import parseConnectionString, getS3Connection

class ProcessedClusterStore:
  """
  Store for processed clusters.

  Contains functions for CRUD operations on processed clusters
  """

  def __init__(self):
    self.bucketConnString = os.environ['PROCESSEDCLUSTERS_BUCKET']

  def __getBucket(self):
    bucketConnParams = parseConnectionString(self.bucketConnString)
    conn = getS3Connection(self.bucketConnString)

    return conn.get_bucket(bucketConnParams['bucketName'], validate=False)

  def getProcessedCluster(self, cluster):
    """
    If the cluster has been previously processed and cached, returns cached result.
    Otherwise, processes if freshly and returns the result after saving to cache
    """

    clusterId = cluster.id
    try:
      k = Key(self.__getBucket())
      k.key = clusterId
      keyContents = k.get_contents_as_string()
      logging.info("Preprocessed cluster found for: " + clusterId)

      preProcessedCluster = Cluster([])
      preProcessedCluster.deserializeFromString(keyContents)
      return preProcessedCluster
    except S3ResponseError:
      logging.info("Preprocessed cluster not found for: " + clusterId)
      return self.processAndSaveCluster(cluster)

  def processAndSaveCluster(self, cluster):
    """
    Process the cluster and store it in cache.
    Returns the processed cluster.
    """
    clusterId = cluster.id
    k = Key(self.__getBucket())
    k.key = clusterId

    cluster.process()
    logging.info("Processed cluster with id: " + clusterId)
    k.set_contents_from_string(cluster.serializeToString())
    logging.info("Processed cluster saved for: " + clusterId)
    return cluster

  def deleteClusters(self, clusters):
    keysToDelete = [cluster.id for cluster in clusters]
    bucket = self.__getBucket()
    bucket.delete_keys(keysToDelete)
