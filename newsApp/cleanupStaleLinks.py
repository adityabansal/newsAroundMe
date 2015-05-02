from linkManager import LinkManager
from loggingHelper import *

InitLogging()

def cleanupStaleLinks():
    """
    Cleanup old links in the links table.
    Run this job periodically.
    """

    linkManager = LinkManager()
    
    logging.info("Getting stale  links.")
    staleLinks = linkManager.getStaleLinks()
    logging.info("Number of stale links are: %i", len(staleLinks))

    for linkId in staleLinks:
        linkManager.delete(linkId)
        logging.info("Deleted link with id: %s", linkId);

if __name__ == '__main__':
    cleanupStaleLinks();
