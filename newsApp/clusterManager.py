import os
import json
import random
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from constants import *
from cachingHelper import getCache
from dbhelper import *
from cluster import Cluster
from clusterTableManager import ClusterTableManager
from loggingHelper import *
from jobManager import JobManager
from workerJob import WorkerJob

#constants
NEW_CLUSTER_FOLDER = "new/"
PROCESSED_CLUSTERS_FOLDER = "processedClusters/"
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
        metadata = eval(self.__getObject(NEW_CLUSTER_FOLDER + METADATA_FILE))
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
        self.__putObject(
            PROCESSED_CLUSTERS_FOLDER + cluster.id,
            json.dumps(cluster.articles))

    def getProcessedCluster(self, clusterId):
        key = PROCESSED_CLUSTERS_FOLDER + clusterId

        value = self.cache.get(key)
        if not value:
          value = self.__getObject(PROCESSED_CLUSTERS_FOLDER + clusterId)
          self.cache.set(key, value, time = 7200)

        return json.loads(value)

    def __computeClusterRankingScore(self, cluster):
        return len(cluster)

    def __constructQueryResponse(self, clusters, skip, top):
        response = []
        clusterList = list(clusters)
        clusterList.sort(key = self.__computeClusterRankingScore, reverse=True)

        for cluster in clusterList[skip:top]:
            try:
                response.append(self.getProcessedCluster(cluster.id))
            except:
                continue

        return response;

    def queryByCategoryAndCountry(self, category, country, skip = 0, top = 5):
        clusters = self.clusterTableManager.queryByCategoryAndCountry(
            category,
            country)
        return self.__constructQueryResponse(clusters, skip, top)

    def queryByLocale(self, locale, skip = 0, top = 5):
        clusters = self.clusterTableManager.queryByLocale(locale)
        response = []

        return self.__constructQueryResponse(clusters, skip, top)

    def putClusters(self, clusters):
        jobManager = JobManager()

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
        return eval(self.__getObject(NEW_CLUSTER_FOLDER + CLUSTERS_FILE))

    def setState(self, state):
        self.__setMetadata(METADATA_NAME_STATE, state)

    def getState(self):
        return self.__getMetadata(METADATA_NAME_STATE)
