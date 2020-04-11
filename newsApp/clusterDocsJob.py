from .constants import *
from .loggingHelper import *
from .clusteringJobs import clusterDocs

InitLogging()

def clusterDocsJob():
    clusterDocs("clusterDocs")

if __name__ == '__main__':
    clusterDocsJob()
