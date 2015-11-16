import os
import json
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from constants import *
from dbhelper import *
from cluster import Cluster

#constants
NEW_CLUSTER_FOLDER = "new/"
DOCLIST_FILE = "docList"
CLUSTERS_FILE = "clusters"
STATE_FILE = "state"

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
            NEW_CLUSTER_FOLDER + STATE_FILE,
            archiveFolderName + STATE_FILE)

    def __cleanupOldDocsFromCluster(self, expiredDocs):
        clusters = self.getClusters()

        newClusters = [(cluster - expiredDocs) for cluster in clusters
                    if (cluster - expiredDocs)]
        self.putClusters(newClusters)

    def __addNewDocsToCluster(self, newDocs):
        clusters = self.getClusters() + [Cluster([docId]) for docId in newDocs]
        self.putClusters(clusters)

    def initNewClusters(self, docList):
        # initialize doc list
        self.putDocList(docList)

        # initizalize each point as separate cluster
        clusters = [Cluster([docId]) for docId in docList]
        self.putClusters(clusters)

        # set state
        self.setState(CLUSTER_STATE_NEW)

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
        self.setState(CLUSTER_STATE_NEW)

        return (list(newDocs), list(retainedDocs), list(expiredDocs))

    def putDocList(self, docList):
        self.__putObject(NEW_CLUSTER_FOLDER + DOCLIST_FILE, json.dumps(docList))

    def getDocList(self):
        return json.loads(self.__getObject(NEW_CLUSTER_FOLDER + DOCLIST_FILE))

    def putClusters(self, clusters):
        self.__putObject(NEW_CLUSTER_FOLDER + CLUSTERS_FILE, str(clusters))

    def getClusters(self):
        return eval(self.__getObject(NEW_CLUSTER_FOLDER + CLUSTERS_FILE))

    def setState(self, state):
        self.__putObject(NEW_CLUSTER_FOLDER + STATE_FILE, state)

    def getState(self):
        return self.__getObject(NEW_CLUSTER_FOLDER + STATE_FILE)
