import os
import json
import random
import time

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key

from constants import *
from docManager import DocManager
from dbhelper import *
from cluster import Cluster
from clusterTableManager import ClusterTableManager
from loggingHelper import *
from clusterJobManager import ClusterJobManager
from workerJob import WorkerJob
from notificationTableManager import NotificationTableManager, Notifier
from notifierTwitter import NotifierTwitter

NOTIFICATION_IMPORTANCE_THRESHOLD = 1.95

class ClusterManager:
    """
    Manage clusters stored in cloud.
    """

    def __init__(self):
        """
        Instantiates a new instance of ClusterManager class

        """

        self.clusterTableManager = ClusterTableManager()
        self.docManager = DocManager()

    def processNewCluster(self, cluster):
        cluster.process()
        cluster.isCurrent = 'true'
        self.clusterTableManager.addCluster(cluster)

    def __getProcessedClusterArticles(self, cluster):
        cluster.process();
        return cluster.articles;

    def __getClusterResponse(self, cluster, filters = None):
        articles = self.__getProcessedClusterArticles(
            self.__filterDocsInCluster(cluster, filters))

        title = articles[0]['title'];
        description = articles[0]['title'] + " - " + \
            articles[0]['publisher'][PUBLISHER_DETAILS_NAME] + ".";
        if len(articles) > 1:
            description += " " + articles[1]['title'] + " - " + \
              articles[1]['publisher'][PUBLISHER_DETAILS_NAME] + ".";

        return {
            "articles": articles,
            "title": title,
            "description": description,
            "locales": cluster.locales,
            "languages": cluster.languages,
            "importance": self.__computeClusterRankingScore(cluster)
        }

    def __computeClusterRankingScore(self, cluster):
        return (0.3 * (len(cluster) - len(cluster.duplicates))) + \
            (0.7 * len(cluster.publishers))

    def __sortClustersByImportance(self, clusters):
        clusterList = list(clusters)
        clusterList.sort(key = self.__computeClusterRankingScore, reverse=True)
        return clusterList;

    def __filterClusters(self, clusterList, filters):
        if not filters:
            return clusterList

        if CLUSTERS_FILTER_LANGUAGES in filters:
            clusterList = [cluster for cluster in clusterList if not \
                set(filters[CLUSTERS_FILTER_LANGUAGES]).isdisjoint(cluster.languages)]

        return clusterList;

    def __filterDocsInCluster(self, cluster, filters):
        if not filters:
            return cluster

        filteredDocs = []

        for docKey in cluster:
            isDocAllowed = True;
            doc = self.docManager.get(docKey)

            if CLUSTERS_FILTER_LANGUAGES in filters:
                if doc.tags[FEEDTAG_LANG] not in filters[CLUSTERS_FILTER_LANGUAGES]:
                    isDocAllowed = False

            if isDocAllowed:
                filteredDocs.append(docKey)

        return Cluster(filteredDocs)

    def __constructQueryResponse(self, clusters, skip, top, filters=None):
        response = []
        clusterList = list(clusters)
        clusterList = self.__filterClusters(clusterList, filters)
        clusterList = self.__sortClustersByImportance(clusterList)

        for cluster in clusterList[skip:(skip + top)]:
            try:
                response.append(self.__getClusterResponse(cluster, filters))

            except Exception, e:
                logging.exception(
                    "Could not construct query response for cluster id %s",
                    cluster.id);
                continue

        return response;

    def queryByCategoryAndCountry(self, category, country, skip, top, filters=None):
        clusters = self.clusterTableManager.queryByCategoryAndCountry(
            category,
            country)
        return self.__constructQueryResponse(clusters, skip, top, filters)

    def queryByLocale(self, locale, skip, top, filters=None):
        clusters = self.clusterTableManager.queryByLocale(locale)
        response = []

        return self.__constructQueryResponse(clusters, skip, top, filters)

    def queryByDocId(self, docId, filters=None):
        cluster = self.clusterTableManager.queryByDocId(docId)
        if not cluster:
            return None
        else:
            return self.__constructQueryResponse([cluster], 0, 1, filters)[0]

    def putCurrentClusters(self, clusters):
        jobManager = ClusterJobManager()

        existingClusters = list(self.getCurrentClusters())
        newClusters = [cluster for cluster in clusters
            if cluster not in existingClusters]
        expiredClusters = [cluster for cluster in existingClusters
            if cluster not in clusters]

        for cluster in newClusters:
            job = WorkerJob(
                JOB_PROCESSNEWCLUSTER,
                { JOBARG_PROCESSNEWCLUSTER_CLUSTER : list(cluster)})
            jobManager.enqueueJob(job)
            logging.info(
                "Put process new cluster job. Cluster id: %s.",
                cluster.id)

        logging.info("Number of clusters to delete are: %i", len(expiredClusters))
        self.clusterTableManager.deleteClusters(expiredClusters)

    def getCurrentClusters(self):
        return self.clusterTableManager.getCurrentClusters()

    def getCurrentDocs(self):
        currentClusters = self.getCurrentClusters()
        return (doc for cluster in currentClusters for doc in cluster)

    def archiveOldClusters(self):
        return self.clusterTableManager.archiveOldClusters();

    def reprocessCurrentClusters(self):
        currentClusters = self.getCurrentClusters()
        for cluster in currentClusters:
            cluster.process()

        self.clusterTableManager.addClusters(currentClusters)

    def notifyTwitterForLocale(self, locale):
        jobId = "notifyTwitterForLocale" + locale
        nt = NotifierTwitter()
        notificationTableManager = NotificationTableManager()

        if not nt.doesLocaleExist(locale):
          logging.info("No twitter handle exists for locale %s. %s", locale, jobId)
          return; #skip

        if nt.isNightTime(locale):
          logging.info("Night time for locale %s. %s", locale, jobId)
          return; #skip

        logging.info("Fetching clusters for locale %s. %s", locale, jobId);
        clusters = clusters = self.clusterTableManager.queryByLocale(locale)
        clustersToNotify = [cluster for cluster in clusters if \
            self.__computeClusterRankingScore(cluster) > NOTIFICATION_IMPORTANCE_THRESHOLD and \
            not notificationTableManager.isClusterNotified(cluster, Notifier.twitter)]
        logging.info("Fetched clusters for locale %s. %s", locale, jobId);
        logging.info("Number of unnotified clusters are: %i. %s", len(clustersToNotify), jobId)

        for cluster in clustersToNotify[:2]:
            cluster.process()
            try:
                nt.notifyForLocale(jobId, cluster, locale)
                notificationTableManager.setClusterNotified(cluster, Notifier.twitter)
            except:
                logging.exception('Failed to tweet story for cluster %s. %s', cluster.id, jobId)

    def notifyTwitterForAllLocales(self):
        for location in LOCATION_METADATA:
            self.notifyTwitterForLocale(location['value'])
