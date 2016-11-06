import os
import json
import random
import time

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key

from constants import *
from cachingHelper import getCache
from dbhelper import *
from cluster import Cluster
from clusterTableManager import ClusterTableManager
from loggingHelper import *
from clusterJobManager import ClusterJobManager
from workerJob import WorkerJob

#constants
NEW_CLUSTER_FOLDER = "new/"
DOCLIST_FILE = "docList"
CLUSTERS_FILE = "clusters"
METADATA_FILE = "metadata"

METADATA_NAME_STATE = 'state'

class ClusterManager:
    """
    Manage clusters stored in cloud.
    """

    def __init__(self):
        """
        Instantiates a new instance of ClusterManager class

        'bucketConnString' : connection string of s3 bucket in which clusters
        are stored.
        """

        self.bucketConnString = os.environ['CLUSTERSBUCKET_CONNECTIONSTRING'];
        self.clusterTableManager = ClusterTableManager()
        self.cache = getCache()

    def __getBucket(self):
        bucketConnParams = parseConnectionString(self.bucketConnString);

        conn = S3Connection(
            bucketConnParams['accessKeyId'],
            bucketConnParams['secretAccessKey']);

        return conn.get_bucket(bucketConnParams['bucketName']);

    def __putObject(self, key, content):
	k = Key(self.__getBucket());
        k.key = key;
        k.set_contents_from_string(content)

    def __getObject(self, docKey):
        k = Key(self.__getBucket());
        k.key = docKey;
        return k.get_contents_as_string()

    def __copyObject(self, fromKey, toKey):
        bucket = self.__getBucket()
        source_key = bucket.get_key(fromKey)

        source_key.copy(bucket, toKey)

    def __archiveCluster(self):
        """
        Save a copy of current clusters in a new folder.
        """

        archiveFolderName = str(int(time.time())) + '/'

        self.__copyObject(
            NEW_CLUSTER_FOLDER + DOCLIST_FILE,
            archiveFolderName + DOCLIST_FILE)
        self.__copyObject(
            NEW_CLUSTER_FOLDER + CLUSTERS_FILE,
            archiveFolderName + CLUSTERS_FILE)
        self.__copyObject(
            NEW_CLUSTER_FOLDER + METADATA_FILE,
            archiveFolderName + METADATA_FILE)

    def __cleanupOldDocsFromCluster(self, expiredDocs):
        clusters = self.getClusters()

        newClusters = [Cluster(list(cluster - expiredDocs)) for cluster in clusters
                    if (cluster - expiredDocs)]
        self.putClusters(newClusters)

    def __addNewDocsToCluster(self, newDocs):
        clusters = self.getClusters() + [Cluster([docId]) for docId in newDocs]
        self.putClusters(clusters)

    def __getMetadata(self, key):
        metadata = eval(self.__getObject(NEW_CLUSTER_FOLDER + METADATA_FILE))

        try:
          return metadata[key]
        except KeyError:
          return None;

    def __setMetadata(self, key, value):
        try:
            metadata = eval(self.__getObject(NEW_CLUSTER_FOLDER + METADATA_FILE))
        except S3ResponseError:
            metadata = {}

        metadata[key] = value
        self.__putObject(NEW_CLUSTER_FOLDER + METADATA_FILE, str(metadata))

    def initNewClusters(self, docList):
        # initialize doc list
        self.putDocList(docList)

        # initizalize each point as separate cluster
        clusters = [Cluster([docId]) for docId in docList]
        self.putClusters(clusters)

        # set state
        self.setState(CLUSTER_STATE_INITIALIZED)

    def initNewIncrementalCluster(self, docList):
        oldDocList = self.getDocList()

        docSet = set(docList)
        oldDocSet = set(oldDocList)

        newDocs = docSet - oldDocSet
        retainedDocs = docSet.intersection(oldDocSet)
        expiredDocs = oldDocSet - docSet

        self.__archiveCluster()

        self.putDocList(docList)
        self.__cleanupOldDocsFromCluster(expiredDocs)
        self.__addNewDocsToCluster(newDocs)
        self.setState(CLUSTER_STATE_INITIALIZED)

        return (list(newDocs), list(retainedDocs), list(expiredDocs))

    def putDocList(self, docList):
        self.__putObject(
            NEW_CLUSTER_FOLDER + DOCLIST_FILE,
            json.dumps(docList))

    def getDocList(self):
        return json.loads(self.__getObject(NEW_CLUSTER_FOLDER + DOCLIST_FILE))

    def processNewCluster(self, cluster):
        cluster.process()
        self.clusterTableManager.addCluster(cluster)

    def getProcessedCluster(self, clusterId):
        cluster = self.clusterTableManager.getCluster(clusterId);
        cluster.process();
        return cluster.articles;

    def __computeClusterRankingScore(self, cluster):
        return len(cluster) - len(cluster.duplicates) + \
            (0.1 * len(cluster.publishers))

    def __constructQueryResponse(self, clusters, skip, top):
        response = []
        clusterList = list(clusters)
        clusterList.sort(key = self.__computeClusterRankingScore, reverse=True)

        for cluster in clusterList[skip:(skip + top)]:
            try:
                response.append(self.getProcessedCluster(cluster.id))
            except:
                continue

        return response;

    def queryByCategoryAndCountry(self, category, country, skip, top):
        clusters = self.clusterTableManager.queryByCategoryAndCountry(
            category,
            country)
        return self.__constructQueryResponse(clusters, skip, top)

    def queryByLocale(self, locale, skip, top):
        clusters = self.clusterTableManager.queryByLocale(locale)
        response = []

        return self.__constructQueryResponse(clusters, skip, top)

    def putClusters(self, clusters):
        jobManager = ClusterJobManager()

        existingClusters = self.getClusters()
        newClusters = [cluster for cluster in clusters
            if cluster not in existingClusters]
        expiredClusters = [cluster for cluster in existingClusters
            if cluster not in clusters]

        for cluster in newClusters:
            job = WorkerJob(
                JOB_PROCESSNEWCLUSTER,
                { JOBARG_PROCESSNEWCLUSTER_CLUSTER : str(cluster)})
            jobManager.enqueueJob(job)
            logging.info(
                "Put process new cluster job. Cluster id: %s.",
                cluster.id)

        self.clusterTableManager.deleteClusters(expiredClusters)

        self.__putObject(NEW_CLUSTER_FOLDER + CLUSTERS_FILE, str(clusters))

    def getClusters(self):
        try:
            return eval(self.__getObject(NEW_CLUSTER_FOLDER + CLUSTERS_FILE))
        except S3ResponseError:
            return []

    def setState(self, state):
        self.__setMetadata(METADATA_NAME_STATE, state)

    def getState(self):
        return self.__getMetadata(METADATA_NAME_STATE)
