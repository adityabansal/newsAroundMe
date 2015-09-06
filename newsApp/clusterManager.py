import os
import json

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from dbhelper import *
from cluster import Cluster

#constants
NEW_CLUSTER_FOLDER = "new/"
DOCLIST_FILE = "docList"
CLUSTERS_FILE = "clusters"

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

    def initNewClusters(self, docList):
        # initialize doc list
        self.__putObject(NEW_CLUSTER_FOLDER + DOCLIST_FILE, json.dumps(docList))

        # initizalize each point as separate cluster
        clusters = [Cluster([docId]) for docId in docList]
        self.putClusters(clusters)

    def getDocList(self):
        return json.loads(self.__getObject(NEW_CLUSTER_FOLDER + DOCLIST_FILE))

    def putClusters(self, clusters):
        self.__putObject(NEW_CLUSTER_FOLDER + CLUSTERS_FILE, str(clusters))

    def getClusters(self):
        return eval(self.__getObject(NEW_CLUSTER_FOLDER + CLUSTERS_FILE))
