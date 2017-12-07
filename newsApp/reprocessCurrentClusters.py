from clusterManager import ClusterManager
from loggingHelper import *

InitLogging()

def reprocessCurrentClusters():
  clusterManager = ClusterManager()

  logging.info("Started Reprocessing current clusters.")
  staleClusters = clusterManager.reprocessCurrentClusters()
  logging.info("Completed Reprocessing current clusters")

if __name__ == '__main__':
    reprocessCurrentClusters()