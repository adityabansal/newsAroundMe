from .linkManager import LinkManager
from .loggingHelper import *

InitLogging()

def cleanupStaleLinks():
    """
    Cleanup old links in the links table.
    Run this job periodically.
    """

    linkManager = LinkManager()
    
    logging.info("Getting stale  links.")
    staleLinks = linkManager.getStaleLinks()

    nStaleLinks = 0;
    for linkId in staleLinks:
        logging.info("Deleting link with id: %s", linkId);
        linkManager.delete(linkId)
        nStaleLinks = nStaleLinks + 1;
        logging.info("Deleted link with id: %s", linkId);

    logging.info("Number of stale links deleted were: %i", nStaleLinks)

if __name__ == '__main__':
    cleanupStaleLinks();
