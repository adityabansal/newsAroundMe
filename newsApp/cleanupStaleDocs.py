from docManager import DocManager
from loggingHelper import *

InitLogging()

def cleanupStaleDocs():
    """
    Cleanup old docs.
    Run this job periodically.
    """

    docManager = DocManager()
    
    logging.info("Getting stale docs.")
    staleDocKeys = docManager.getStaleDocKeys()

    for docKey in staleDocKeys:
        logging.info("Deleting doc with key: %s", docKey);
        docManager.delete(docKey);
        logging.info("Deleted doc with key: %s", docKey);

    logging.info("Number of stale docs deleted were: %i", len(staleDocKeys))

if __name__ == '__main__':
    cleanupStaleDocs();