import json
import logging
import os

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key

from .cachingHelper import getCache
from .cluster import Cluster
from .dbhelper import parseConnectionString, getS3Connection

class ProcessedClusterStore:
  """
  Store for processed clusters.

  Contains functions for CRUD operations on processed clusters
  """

  def __init__(self):
    self.bucketConnString = os.environ['PROCESSEDCLUSTERS_BUCKET']
    self.cache = getCache()
    self.__cacheExpiry= 600
    self.__cacheKeyPrefix = 'pc'

  def __getBucket(self):
    bucketConnParams = parseConnectionString(self.bucketConnString)
    conn = getS3Connection(self.bucketConnString)

    return conn.get_bucket(bucketConnParams['bucketName'], validate=False)

  def __getCacheKey(self, clusterId):
    return self.__cacheKeyPrefix + clusterId

  def getProcessedCluster(self, cluster):
    """
    If the cluster has been previously processed and cached, returns cached result.
    Otherwise, processes if freshly and returns the result after saving to cache
    """

    clusterId = cluster.id

    clusterFromCache =  self.cache.get(self.__getCacheKey(clusterId))
    if clusterFromCache:
      logging.info("Preprocessed cluster found in cache for: " + clusterId)

      preProcessedCluster = Cluster([])
      preProcessedCluster.deserializeFromString(clusterFromCache)
      return preProcessedCluster

    try:
      k = Key(self.__getBucket())
      k.key = clusterId
      keyContents = k.get_contents_as_string()
      logging.info("Preprocessed cluster found in S3 for: " + clusterId)

      preProcessedCluster = Cluster([])
      preProcessedCluster.deserializeFromString(keyContents)
      self.cache.set(self.__getCacheKey(clusterId), keyContents, self.__cacheExpiry)
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
    serializedCluster = cluster.serializeToString()

    # Save only large-size clusters in S3 as S3 PUT calls are expensive.
    if len(cluster) > 4:
      k.set_contents_from_string(serializedCluster)
      logging.info("Processed cluster saved for: " + clusterId)

    self.cache.set(self.__getCacheKey(clusterId), serializedCluster, self.__cacheExpiry)
    return cluster

  def deleteClusters(self, clusters):
    keysToDelete = [cluster.id for cluster in clusters]

    self.cache.delete_multi(keysToDelete)

    bucket = self.__getBucket()
    bucket.delete_keys(keysToDelete)
