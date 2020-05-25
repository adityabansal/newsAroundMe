#Initialize the logging.
# InitLogging() should be called at the startup of each process in Procfile

import logging

def InitLogging():
    """
    Initizalize the logging.
    """

    logging.basicConfig(format='%(module)s:%(levelname)s:%(message)s', level=logging.INFO)

    # suppress all logs except critical ones from boto
    logging.getLogger('boto').setLevel(logging.CRITICAL)

    logging.getLogger('bmemcached').setLevel(logging.ERROR)

    logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

    logging.captureWarnings(True)
