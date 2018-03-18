from clusterManager import ClusterManager
from loggingHelper import *

InitLogging()

def notifyTwitter():
    cm = ClusterManager()
    cm.notifyTwitterForAllLocales();


if __name__ == '__main__':
	notifyTwitter()