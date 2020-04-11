from .clusterManager import ClusterManager
from .constants import LOCATION_METADATA
from .loggingHelper import *
from .notificationTableManager import NotificationTableManager, Notifier
from .notifierTwitter import NotifierTwitter

InitLogging()

def notifyTwitterForLocale(locale):
    jobId = "notifyTwitterForLocale" + locale
    nt = NotifierTwitter()
    notificationTableManager = NotificationTableManager()
    clusterManager = ClusterManager()

    if not nt.doesLocaleExist(locale):
        logging.info("No twitter handle exists for locale %s. %s", locale, jobId)
        return #skip

    if nt.isNightTime(locale):
        logging.info("Night time for locale %s. %s", locale, jobId)
        return #skip

    logging.info("Fetching notifiable clusters for locale %s. %s", locale, jobId)
    clusters = clusterManager.getNotfiableClustersForLocale(jobId, locale)
    logging.info("Fetched notifiable clusters for locale %s. %s", locale, jobId)
    clustersToNotify = [cluster for cluster in clusters if \
        not notificationTableManager.isClusterNotified(cluster, Notifier.twitter)]
    logging.info("Number of unnotified clusters are: %i. %s", len(clustersToNotify), jobId)

    for cluster in clustersToNotify[:2]:
        cluster = clusterManager.getProcessedCluster(cluster)
        try:
            nt.notifyForLocales(jobId, cluster)
            notificationTableManager.setClusterNotified(cluster, Notifier.twitter)
        except:
            logging.exception('Failed to tweet story for cluster %s. %s', cluster.id, jobId)

def notifyTwitterForAllLocales():
    for location in LOCATION_METADATA:
        notifyTwitterForLocale(location['value'])

def notifyTwitter():
    notifyTwitterForAllLocales()

if __name__ == '__main__':
	notifyTwitterForAllLocales()